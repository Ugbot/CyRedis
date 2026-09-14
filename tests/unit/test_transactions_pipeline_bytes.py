"""Connection affinity, dirty-connection handling, pipeline draining and
binary-safe replies for the native client.

Every test uses ``max_connections=1`` so that any leaked transaction state
or unread reply on the single pooled connection is observed by the very
next command.
"""

import pytest

from cy_redis.core.cy_redis_client import CyRedisClient, RedisError
from tests.server_env import REDIS_HOST, REDIS_PORT

pytestmark = pytest.mark.redis


@pytest.fixture
def client():
    c = CyRedisClient(host=REDIS_HOST, port=REDIS_PORT, max_connections=1)
    yield c
    keys = c.keys("test:txpb:*")
    if keys:
        c.delete(*keys)
    c.close()


@pytest.fixture
def raw_client():
    c = CyRedisClient(
        host=REDIS_HOST, port=REDIS_PORT, max_connections=1, decode_responses=False
    )
    yield c
    keys = c.keys(b"test:txpb:*")
    if keys:
        c.delete(*keys)
    c.close()


def _only_pooled_connection(client):
    conns = client.pool.get_connections()
    assert len(conns) == 1, "expected exactly one idle pooled connection"
    return conns[0]


# ---------------------------------------------------------------------------
# Pipeline draining
# ---------------------------------------------------------------------------


def test_pipeline_error_drains_all_replies(client):
    key = "test:txpb:str"
    with client.pipeline() as pipe:
        pipe.set(key, "x")
        pipe.lpush(key, "y")  # WRONGTYPE
        pipe.get(key)
        with pytest.raises(RedisError, match="WRONGTYPE"):
            pipe.execute()
    # The next standalone command must see its own reply, not a leftover.
    assert client.get(key) == "x"
    assert client.ping() is True
    assert _only_pooled_connection(client).pending_replies == 0


def test_pipeline_raise_on_error_false_returns_error_objects(client):
    key = "test:txpb:str2"
    with client.pipeline() as pipe:
        pipe.set(key, "x")
        pipe.lpush(key, "y")
        pipe.get(key)
        results = pipe.execute(raise_on_error=False)
    assert results[0] is True
    assert isinstance(results[1], RedisError)
    assert results[2] == "x"


def test_pipeline_multi_runtime_error_keeps_connection_in_sync(client):
    key = "test:txpb:multi_err"
    with client.pipeline() as pipe:
        pipe.multi()
        pipe.set(key, "x")
        pipe.lpush(key, "y")  # queued fine, fails at EXEC time
        pipe.get(key)
        results = pipe.execute(raise_on_error=False)
    assert results[0] is True
    assert isinstance(results[1], RedisError)
    assert results[2] == "x"
    assert client.get(key) == "x"
    conn = _only_pooled_connection(client)
    assert conn.is_clean()


def test_pipeline_multi_queue_error_raises_real_reason(client):
    with client.pipeline() as pipe:
        pipe.multi()
        pipe.execute_command("SET", "only-one-arg")
        with pytest.raises(RedisError, match="wrong number of arguments"):
            pipe.execute()
    assert client.ping() is True
    assert _only_pooled_connection(client).is_clean()


# ---------------------------------------------------------------------------
# Transaction affinity and dirty-connection handling
# ---------------------------------------------------------------------------


def test_transaction_pins_one_connection_for_watch_multi_exec(client):
    key = "test:txpb:counter"
    client.set(key, "5")
    with client.transaction() as tx:
        tx.watch(key)
        current = int(tx.get(key))
        tx.multi()
        tx.set(key, current + 1)
        tx.incr(key)
        assert tx.execute() == [True, 7]
    assert client.get(key) == "7"


def test_transaction_aborts_when_watched_key_changes(client):
    key = "test:txpb:watched"
    client.set(key, "1")
    other = CyRedisClient(host=REDIS_HOST, port=REDIS_PORT, max_connections=1)
    try:
        with client.transaction() as tx:
            tx.watch(key)
            other.set(key, "changed-elsewhere")
            tx.multi()
            tx.set(key, "from-transaction")
            assert tx.execute() is None
    finally:
        other.close()
    assert client.get(key) == "changed-elsewhere"
    assert _only_pooled_connection(client).is_clean()


def test_abandoned_transaction_does_not_leak_into_next_caller(client):
    key = "test:txpb:abandoned"
    client.set(key, "before")
    with client.transaction() as tx:
        tx.watch(key)
        tx.multi()
        tx.set(key, "never-executed")
        # No execute(): the block is left open on exit.
    # If MULTI had leaked, this SET would answer QUEUED and GET would too.
    assert client.set(key, "after") is True
    assert client.get(key) == "after"
    conn = _only_pooled_connection(client)
    assert conn.is_clean()
    assert not conn.in_multi
    assert not conn.watching


def test_exception_inside_transaction_body_releases_clean_connection(client):
    key = "test:txpb:raise"
    with pytest.raises(RuntimeError):
        with client.transaction() as tx:
            tx.watch(key)
            tx.multi()
            tx.set(key, "x")
            raise RuntimeError("caller bug")
    assert client.get(key) is None
    assert _only_pooled_connection(client).is_clean()


def test_pipeline_discard_clears_state(client):
    key = "test:txpb:discard"
    with client.transaction() as tx:
        tx.watch(key)
        tx.multi()
        tx.set(key, "x")
        tx.discard()
        assert tx.execute() == []
    assert client.get(key) is None


def test_connection_state_tracking_on_raw_commands(client):
    conn = client.pool.get_connection()
    try:
        assert conn.is_clean()
        assert conn.execute_command(["WATCH", "test:txpb:w"]) == "OK"
        assert conn.watching
        assert conn.execute_command(["MULTI"]) == "OK"
        assert conn.in_multi
        assert conn.execute_command(["SET", "test:txpb:w", "1"]) == "QUEUED"
        assert conn.execute_command(["DISCARD"]) == "OK"
        assert conn.is_clean()
    finally:
        client.pool.return_connection(conn)


def test_client_level_multi_exec_removed(client):
    for name in ("multi", "exec_", "watch", "unwatch", "discard"):
        assert not hasattr(client, name), name


# ---------------------------------------------------------------------------
# Binary safety
# ---------------------------------------------------------------------------


def test_bytes_mode_round_trips_arbitrary_binary(raw_client):
    key = b"test:txpb:\x00\xff"
    value = bytes(range(256))
    assert raw_client.set(key, value) is True
    assert raw_client.get(key) == value
    assert raw_client.exists(key) == 1


def test_bytes_mode_returns_bytes_for_aggregates(raw_client):
    key = b"test:txpb:hash"
    raw_client.hset(key, mapping={"a": b"\xff", b"\x00": "text"})
    result = raw_client.hgetall(key)
    assert result == {b"a": b"\xff", b"\x00": b"text"}
    raw_client.sadd(b"test:txpb:set", b"\xfe", "s")
    assert set(raw_client.smembers(b"test:txpb:set")) == {b"\xfe", b"s"}


def test_bytes_mode_keeps_protocol_text_as_str(raw_client):
    assert raw_client.ping() is True
    assert raw_client.set(b"test:txpb:status", b"v") is True
    with pytest.raises(RedisError) as excinfo:
        raw_client.execute_command("LPUSH", b"test:txpb:status", b"x")
    assert "WRONGTYPE" in str(excinfo.value)


def test_decode_mode_is_strict_utf8(client):
    key = "test:txpb:notutf8"
    client.set(key, b"\xff\x00")
    with pytest.raises(UnicodeDecodeError):
        client.get(key)
    # The failed decode consumed its reply; the connection is still usable.
    assert client.ping() is True


def test_bytes_arguments_are_not_str_coerced(client):
    key = "test:txpb:members"
    client.sadd(key, b"m1", "m2")
    assert set(client.smembers(key)) == {"m1", "m2"}
    client.rpush("test:txpb:list", b"a", "b")
    assert client.lrange("test:txpb:list", 0, -1) == ["a", "b"]
