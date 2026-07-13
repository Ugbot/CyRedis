"""
Integration tests for the pgcache Redis module.

Requires:
  - Redis on port 6380 with pgcache.so loaded
  - PostgreSQL reachable at localhost:5432 with the test_users table

Load the module manually before running (substitute your OS username):
    redis-cli -p 6380 MODULE LOAD /path/to/pgcache.so \\
        pg_host localhost pg_port 5432 pg_database postgres \\
        pg_user <user> pg_password "" default_ttl 60

Run:
    pytest tests/integration/test_pgcache_module.py -v -m pgcache
"""

import json
import os
import subprocess
import uuid

import pytest

try:
    import redis as redis_py

    REDIS_PY_AVAILABLE = True
except ImportError:
    REDIS_PY_AVAILABLE = False

MODULE_REDIS_PORT = int(os.getenv("CY_GAME_REDIS_PORT", "6380"))
PGCACHE_SO_PATH = os.path.abspath("plugins/pgcache/src/pgcache.so")

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def module_redis():
    if not REDIS_PY_AVAILABLE:
        pytest.skip("redis-py not installed")
    try:
        r = redis_py.Redis(
            host="127.0.0.1",
            port=MODULE_REDIS_PORT,
            decode_responses=True,
            socket_timeout=5,
        )
        r.ping()
        return r
    except Exception:
        pytest.skip(f"Redis not available on port {MODULE_REDIS_PORT}")


@pytest.fixture(scope="session")
def pgcache_loaded(module_redis):
    """Ensure pgcache module is loaded; skip if not."""
    mods = module_redis.execute_command("MODULE", "LIST")
    names = []
    for i in range(0, len(mods), 2):
        item = mods[i]
        if isinstance(item, (list, tuple)):
            # Flat key/value pairs inside each module entry
            for j in range(0, len(item), 2):
                if item[j] == "name":
                    names.append(item[j + 1])
        elif item == "name" and i + 1 < len(mods):
            names.append(mods[i + 1])

    if "pgcache" not in names:
        if not os.path.exists(PGCACHE_SO_PATH):
            pytest.skip(f"pgcache.so not built at {PGCACHE_SO_PATH}")
        pg_user = os.getenv("PGUSER", os.popen("whoami").read().strip())
        try:
            module_redis.execute_command(
                "MODULE",
                "LOAD",
                PGCACHE_SO_PATH,
                "pg_host",
                "localhost",
                "pg_port",
                "5432",
                "pg_database",
                "postgres",
                "pg_user",
                pg_user,
                "pg_password",
                "",
                "default_ttl",
                "60",
            )
        except Exception as e:
            pytest.skip(f"Could not load pgcache module: {e}")

    return module_redis


@pytest.fixture(scope="session")
def pg_table(pgcache_loaded):
    """Create and seed the test_users table via psql; yield table name."""
    pg_user = os.getenv("PGUSER", os.popen("whoami").read().strip())
    ddl = """
DROP TABLE IF EXISTS pgcache_test_users;
CREATE TABLE pgcache_test_users (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT,
    score INTEGER DEFAULT 0
);
INSERT INTO pgcache_test_users (id, name, email, score) VALUES
    (1, 'Alice', 'alice@example.com', 100),
    (2, 'Bob',   'bob@example.com',   200),
    (3, 'Carol', 'carol@example.com', 150);
"""
    result = subprocess.run(
        ["psql", "-U", pg_user, "-d", "postgres", "-c", ddl],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"Could not create test table: {result.stderr}")

    yield "pgcache_test_users"

    # Teardown
    subprocess.run(
        [
            "psql",
            "-U",
            pg_user,
            "-d",
            "postgres",
            "-c",
            "DROP TABLE IF EXISTS pgcache_test_users;",
        ],
        capture_output=True,
    )


@pytest.fixture(autouse=True)
def flush_cache_keys(pgcache_loaded, request):
    """Remove cached keys before each test so cache is cold."""
    if "pg_table" not in request.fixturenames:
        return
    table = "pgcache_test_users"
    prefix = "pg_cache:"
    keys = pgcache_loaded.keys(f"{prefix}{table}:*")
    if keys:
        pgcache_loaded.delete(*keys)


# ── Helpers ───────────────────────────────────────────────────────────────────


def read(r, table, pk_dict, ttl=None):
    args = ["PGCACHE.READ", table, json.dumps(pk_dict, separators=(",", ":"))]
    if ttl is not None:
        args.append(str(ttl))
    return r.execute_command(*args)


def write(r, table, pk_dict, data_dict, ttl=None):
    args = [
        "PGCACHE.WRITE",
        table,
        json.dumps(pk_dict, separators=(",", ":")),
        json.dumps(data_dict, separators=(",", ":")),
    ]
    if ttl is not None:
        args.append(str(ttl))
    return r.execute_command(*args)


def invalidate(r, table, pk_dict):
    return r.execute_command(
        "PGCACHE.INVALIDATE",
        table,
        json.dumps(pk_dict, separators=(",", ":")),
    )


def multiread(r, table, pk_list, ttl=None):
    args = ["PGCACHE.MULTIREAD", table, json.dumps(pk_list, separators=(",", ":"))]
    if ttl is not None:
        args.append(str(ttl))
    return r.execute_command(*args)


# ── Tests: WRITE and READ ─────────────────────────────────────────────────────


@pytest.mark.pgcache
class TestWriteRead:
    def test_write_returns_ok(self, pgcache_loaded):
        r = pgcache_loaded
        result = write(
            r,
            "pgcache_test_users",
            {"id": "99"},
            {"id": "99", "name": "Test", "score": "0"},
        )
        assert result == "OK"

    def test_read_cache_hit(self, pgcache_loaded):
        r = pgcache_loaded
        data = {"id": "42", "name": "HitUser", "email": "hit@test.com", "score": "7"}
        write(r, "pgcache_test_users", {"id": "42"}, data, 120)
        result = read(r, "pgcache_test_users", {"id": "42"})
        assert result is not None
        row = json.loads(result)
        assert row["id"] == "42"
        assert row["name"] == "HitUser"
        assert row["email"] == "hit@test.com"

    def test_read_custom_ttl(self, pgcache_loaded):
        r = pgcache_loaded
        write(r, "pgcache_test_users", {"id": "43"}, {"id": "43", "name": "TTLUser"}, 5)
        result = read(r, "pgcache_test_users", {"id": "43"})
        assert result is not None

    def test_write_overwrites_existing(self, pgcache_loaded):
        r = pgcache_loaded
        write(
            r, "pgcache_test_users", {"id": "10"}, {"id": "10", "name": "OldName"}, 120
        )
        write(
            r, "pgcache_test_users", {"id": "10"}, {"id": "10", "name": "NewName"}, 120
        )
        result = read(r, "pgcache_test_users", {"id": "10"})
        row = json.loads(result)
        assert row["name"] == "NewName"


# ── Tests: INVALIDATE ─────────────────────────────────────────────────────────


@pytest.mark.pgcache
class TestInvalidate:
    def test_invalidate_removes_from_cache(self, pgcache_loaded):
        r = pgcache_loaded
        write(
            r, "pgcache_test_users", {"id": "50"}, {"id": "50", "name": "ToDelete"}, 120
        )
        assert read(r, "pgcache_test_users", {"id": "50"}) is not None
        invalidate(r, "pgcache_test_users", {"id": "50"})
        # Without postgres having this row, cache miss returns None
        # (or the actual row if postgres has it — we just check the cache key is gone)
        cache_key = f'pg_cache:pgcache_test_users:{{"id":"50"}}'
        assert r.exists(cache_key) == 0

    def test_invalidate_nonexistent_key_is_noop(self, pgcache_loaded):
        r = pgcache_loaded
        result = invalidate(r, "pgcache_test_users", {"id": "9999"})
        assert result == "OK"


# ── Tests: READ from PostgreSQL (cache-miss path) ─────────────────────────────


@pytest.mark.pgcache
class TestReadFromPostgres:
    def test_read_cache_miss_fetches_from_pg(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        # Cache is cold (flush_cache_keys runs before each test)
        result = read(r, pg_table, {"id": "1"})
        assert result is not None
        row = json.loads(result)
        assert row["id"] == "1"
        assert row["name"] == "Alice"
        assert row["email"] == "alice@example.com"
        assert row["score"] == "100"

    def test_read_populates_cache(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        read(r, pg_table, {"id": "2"})
        # Second read should hit cache (key now exists)
        pk_str = json.dumps({"id": "2"}, separators=(",", ":"))
        cache_key = f"pg_cache:{pg_table}:{pk_str}"
        assert r.exists(cache_key) == 1

    def test_read_second_call_is_cache_hit(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        first = read(r, pg_table, {"id": "3"})
        second = read(r, pg_table, {"id": "3"})
        assert first == second
        row = json.loads(second)
        assert row["name"] == "Carol"

    def test_read_missing_row_returns_none(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        result = read(r, pg_table, {"id": "9999"})
        assert result is None

    def test_read_all_columns_present(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        result = read(r, pg_table, {"id": "1"})
        row = json.loads(result)
        assert set(row.keys()) == {"id", "name", "email", "score"}


# ── Tests: MULTIREAD ─────────────────────────────────────────────────────────


@pytest.mark.pgcache
class TestMultiRead:
    def test_multiread_cache_hits(self, pgcache_loaded):
        r = pgcache_loaded
        for i in range(1, 4):
            write(
                r,
                "pgcache_test_users",
                {"id": str(i)},
                {"id": str(i), "name": f"User{i}"},
                120,
            )

        results = multiread(
            r, "pgcache_test_users", [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        )
        assert len(results) == 3
        for res, expected_id in zip(results, ["1", "2", "3"]):
            assert res is not None
            row = json.loads(res)
            assert row["id"] == expected_id

    def test_multiread_from_postgres(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        results = multiread(r, pg_table, [{"id": "1"}, {"id": "2"}, {"id": "3"}])
        assert len(results) == 3
        names = [json.loads(r)["name"] for r in results if r is not None]
        assert "Alice" in names
        assert "Bob" in names
        assert "Carol" in names

    def test_multiread_missing_returns_none_slot(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        results = multiread(r, pg_table, [{"id": "1"}, {"id": "9999"}])
        assert len(results) == 2
        assert results[0] is not None
        assert results[1] is None

    def test_multiread_mixed_cache_and_miss(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        # Pre-warm id=1
        write(
            r,
            pg_table,
            {"id": "1"},
            {"id": "1", "name": "Alice", "email": "alice@example.com", "score": "100"},
            120,
        )
        # id=2 stays cold
        results = multiread(r, pg_table, [{"id": "1"}, {"id": "2"}])
        assert len(results) == 2
        assert results[0] is not None
        assert results[1] is not None  # fetched from pg
        assert json.loads(results[1])["name"] == "Bob"

    def test_multiread_custom_ttl(self, pgcache_loaded):
        r = pgcache_loaded
        for i in [20, 21]:
            write(
                r,
                "pgcache_test_users",
                {"id": str(i)},
                {"id": str(i), "name": f"User{i}"},
                300,
            )
        results = multiread(
            r, "pgcache_test_users", [{"id": "20"}, {"id": "21"}], ttl=10
        )
        assert len(results) == 2
        assert all(res is not None for res in results)


# ── Tests: cache key TTL ──────────────────────────────────────────────────────


@pytest.mark.pgcache
class TestTTL:
    def test_cached_key_has_ttl(self, pgcache_loaded, pg_table):
        r = pgcache_loaded
        read(r, pg_table, {"id": "1"})
        pk_str = json.dumps({"id": "1"}, separators=(",", ":"))
        ttl = r.ttl(f"pg_cache:{pg_table}:{pk_str}")
        assert 0 < ttl <= 60  # default_ttl=60

    def test_write_sets_custom_ttl(self, pgcache_loaded):
        r = pgcache_loaded
        write(
            r, "pgcache_test_users", {"id": "60"}, {"id": "60", "name": "TTLTest"}, 300
        )
        pk_str = json.dumps({"id": "60"}, separators=(",", ":"))
        ttl = r.ttl(f"pg_cache:pgcache_test_users:{pk_str}")
        assert 290 <= ttl <= 300


# ── Tests: event publishing ───────────────────────────────────────────────────


@pytest.mark.pgcache
class TestEvents:
    def test_write_publishes_event(self, pgcache_loaded):
        r = pgcache_loaded
        p = r.pubsub()
        p.subscribe("pg_cache_events")
        p.get_message(timeout=0.1)  # subscription confirmation

        write(
            r,
            "pgcache_test_users",
            {"id": "70"},
            {"id": "70", "name": "EventUser"},
            120,
        )

        msg = p.get_message(timeout=2.0)
        p.unsubscribe()
        assert msg is not None
        payload = json.loads(msg["data"])
        assert payload["type"] == "cache_write"
        assert payload["table"] == "pgcache_test_users"

    def test_invalidate_publishes_event(self, pgcache_loaded):
        r = pgcache_loaded
        write(
            r,
            "pgcache_test_users",
            {"id": "71"},
            {"id": "71", "name": "EventUser2"},
            120,
        )

        p = r.pubsub()
        p.subscribe("pg_cache_events")
        p.get_message(timeout=0.1)

        invalidate(r, "pgcache_test_users", {"id": "71"})

        msg = p.get_message(timeout=2.0)
        p.unsubscribe()
        assert msg is not None
        payload = json.loads(msg["data"])
        assert payload["type"] == "cache_invalidate"

    def test_read_hit_publishes_event(self, pgcache_loaded):
        r = pgcache_loaded
        write(
            r, "pgcache_test_users", {"id": "72"}, {"id": "72", "name": "HitEvent"}, 120
        )

        p = r.pubsub()
        p.subscribe("pg_cache_events")
        p.get_message(timeout=0.1)

        read(r, "pgcache_test_users", {"id": "72"})

        msg = p.get_message(timeout=2.0)
        p.unsubscribe()
        assert msg is not None
        payload = json.loads(msg["data"])
        assert payload["type"] == "cache_hit"
