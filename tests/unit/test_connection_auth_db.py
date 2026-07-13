"""Connection-level AUTH and logical-DB (SELECT) support.

These exercise the post-connect handshake added to CyRedisConnection: a
password triggers AUTH and a non-zero db triggers SELECT, both before the
connection is handed out as usable. The db-isolation tests run against the
standard passwordless Redis on localhost:6379.
"""

import uuid

import pytest

from cy_redis.core.cy_redis_client import (
    CyRedisClient,
    CyRedisConnection,
)


def _key():
    return "test:authdb:" + uuid.uuid4().hex


@pytest.mark.redis
def test_db_select_isolates_keys():
    """A key written on db=1 is not visible on db=0 (SELECT was applied)."""
    key = _key()
    c0 = CyRedisClient(host="localhost", port=6379, db=0)
    c1 = CyRedisClient(host="localhost", port=6379, db=1)
    try:
        c1.set(key, "in-db-1")
        assert c1.get(key) == "in-db-1"
        # Same key on db 0 must be absent — proves the connections selected
        # different logical databases.
        assert c0.get(key) is None
    finally:
        c1.delete(key)


@pytest.mark.redis
def test_db_out_of_range_rejected():
    """The pool rejects an impossible db index at construction."""
    with pytest.raises(AssertionError):
        CyRedisClient(host="localhost", port=6379, db=70000)


@pytest.mark.redis
def test_password_against_unsecured_server_fails_cleanly():
    """Supplying a password to a server with no password configured issues
    AUTH, the server rejects it, and we surface a clean ConnectionError
    (never a silently-unauthenticated connection)."""
    from cy_redis.core.cy_redis_client import ConnectionError as CyConnectionError

    c = CyRedisClient(host="localhost", port=6379, password="unexpected-password")
    with pytest.raises(CyConnectionError):
        c.set(_key(), "v")


@pytest.mark.redis
def test_connection_constructor_accepts_password_and_db():
    """CyRedisConnection takes password/db without error and connects."""
    conn = CyRedisConnection("localhost", 6379, 5.0, None, 0)
    assert conn.connect() == 0
    assert conn.connected
    conn.disconnect()
