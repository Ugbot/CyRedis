# Testing

← [README](../README.md)

## Running tests

```bash
# All unit tests (no Redis required)
uv run pytest tests/unit/ -v

# All integration tests (requires Redis on localhost:6379)
uv run pytest tests/integration/ -v

# Specific module
uv run pytest tests/unit/test_channels.py -v

# With coverage
uv run pytest tests/unit/ --cov=cy_redis --cov-report=term-missing
```

## Test layout

```
tests/
  conftest.py              — shared fixtures (redis_client, async client, etc.)
  unit/
    conftest.py            — unit-test fixtures (MockRedisClient, MockWebSocket)
    test_channels.py       — CyChannelManager (35 tests, no Redis needed)
    test_messaging.py      — messaging layer
    test_protocol.py       — RESP protocol parsing
    test_worker_*          — worker queue and coordination
    ...
  integration/
    conftest.py            — integration fixtures (live Redis connection)
    test_worker_coordination.py
    ...
```

## Markers

Tests are marked so you can target exactly what you need:

| Marker | Description |
|--------|-------------|
| `unit` | No external services |
| `integration` | Requires Redis on localhost:6379 |
| `requires_redis` | Same as integration |
| `requires_postgres` | Requires PostgreSQL |
| `slow` | Long-running tests |
| `cluster` | Requires Redis Cluster |
| `sentinel` | Requires Redis Sentinel |
| `messaging` | Messaging / pub-sub tests |
| `worker_coordination` | Worker coordination tests |
| `pgcache` | pgcache plugin tests |
| `skip_ci` | Excluded from CI |

```bash
# Only unit tests
uv run pytest -m unit

# Skip slow tests
uv run pytest -m "not slow"

# Only channel-related tests
uv run pytest tests/unit/test_channels.py
```

## Unit test fixtures

Some unit tests use in-memory mocks — no Redis process required. The channel
tests define `MockRedisClient` and `MockWebSocket` inside
`tests/unit/test_channels.py` (not in `conftest.py`):

```python
# MockRedisClient: simulates the async interface with asyncio.Queue
class MockRedisClient:
    async def publish_async(self, channel, message): ...
    async def xadd_async(self, stream, data, message_id="*"): ...
    async def xread_async(self, streams, count, block): ...
    # ... and the other *_async methods used by the channel manager

# MockWebSocket: asyncio.Queue-backed WebSocket
class MockWebSocket:
    async def accept(self): ...
    async def send_text(self, text): ...
    async def receive_text(self): ...
    async def close(self): ...
```

## Integration test setup

Integration tests hit a real Redis instance. Start one with:

```bash
# Using Docker
docker run -d -p 6379:6379 redis:7-alpine

# Or Valkey (wire-compatible)
docker run -d -p 6379:6379 valkey/valkey:8-alpine
```

The `conftest.py` in `tests/integration/` creates a `CyRedisClient` fixture and flushes the test database between tests.

Every fixture takes its address from `REDIS_HOST`/`REDIS_PORT` (defaults
`localhost:6379`), so the same suite runs against either server:

```bash
docker run -d -p 6381:6379 valkey/valkey:8-alpine
REDIS_PORT=6381 uv run pytest tests/
```

The suite must be green on both — Redis 7 and Valkey 8 pass and skip exactly the
same tests today. Never hardcode `localhost:6379` in a test; import
`REDIS_HOST`/`REDIS_PORT` from `tests/server_env.py`.

`tests/unit/test_module_parity.py` covers the JSON and search modules and skips
the families the configured server does not load, so point it at a
module-bearing image to exercise them:

```bash
docker run -d -p 6383:6379 valkey/valkey-bundle:8
docker run -d -p 6384:6379 redis/redis-stack-server:7.2.0-v11
REDIS_PORT=6383 uv run pytest tests/unit/test_module_parity.py
REDIS_PORT=6384 uv run pytest tests/unit/test_module_parity.py
```

[valkey.md](valkey.md) records which module features exist on each server.

## CI

`.github/workflows/tests.yml` defines three jobs:

- **test** (`ubuntu-latest`): Redis 7-alpine on 6379, Valkey 8-alpine on 6380,
  redis-stack-server on 6382, valkey-bundle on 6383, and PostgreSQL 15 as
  service containers; runs the full suite with coverage against Redis, re-runs
  it with `REDIS_PORT=6380` against Valkey, and runs the module parity tests
  against both module-bearing servers.
- **test-macos** (`macos-latest`, Python 3.11): builds the extensions and runs
  the fast tests (`-m "not slow and not cluster"`). GitHub does not support
  service containers on macOS runners, so tests needing live services may be
  skipped or tolerated.
- **lint**: formatting/lint checks.

```bash
# Run the fast subset locally
uv run pytest tests/ -m "not slow and not cluster"
```

## Native Redis modules

The `cy_game` and `pgcache` tests drive C modules that a stock server does not
carry, so they run against their own server. Both modules are built against
glibc and cannot be relocated into an Alpine image — use the Debian-based
`redis:7`.

```bash
make module        # builds cyredis_game/module/cy_game.so
docker run -d -p 6385:6379 -v "$PWD/cyredis_game/module:/mod:ro" redis:7 \
    redis-server --loadmodule /mod/cy_game.so
CY_GAME_REDIS_PORT=6385 uv run pytest tests/unit/test_physics.py \
    tests/unit/test_goap.py tests/unit/test_pathfinding.py \
    tests/unit/test_flecs_module.py tests/integration/test_cy_game_module.py
```

pgcache also needs libpq and jansson in the server image (see
`tests/docker/pgcache/Dockerfile`) and a PostgreSQL the module and the tests
both reach:

```bash
make -C plugins/pgcache/src
docker build -t cyredis-pgcache tests/docker/pgcache
docker run -d -p 5433:5432 -e POSTGRES_USER=pgcache \
    -e POSTGRES_PASSWORD=pgcache -e POSTGRES_DB=pgcache postgres:16
docker run -d -p 6386:6379 -v "$PWD/plugins/pgcache/src:/mod:ro" cyredis-pgcache
PGCACHE_REDIS_PORT=6386 PGPORT=5433 PGUSER=pgcache PGPASSWORD=pgcache \
    PGDATABASE=pgcache uv run pytest tests/integration/test_pgcache_module.py
```

## Cluster and Sentinel

`tests/integration/test_cluster_operations.py` and `test_sentinel_failover.py`
skip unconditionally: they describe a cluster client and a Sentinel client that
CyRedis does not have. The client exposes the `CLUSTER *` commands, but no slot
map, no MOVED/ASK redirection, and no master discovery, so a single-node client
cannot serve those tests. The configs under `tests/docker/` exist for when
those clients are built; until then the tests document the gap rather than
cover it.

## Adding tests

1. Unit tests go in `tests/unit/`. Use `MockRedisClient` and `MockWebSocket` — avoid real connections.
2. Integration tests go in `tests/integration/`. Mark with `@pytest.mark.integration` and `@pytest.mark.requires_redis`.
3. Use randomized data (not hardcoded values) so tests surface ordering and encoding bugs.
4. Fix what the test reveals — do not adjust assertions to hide failures.

## Plugin tests

The pgcache plugin has its own test suite:

```bash
uv run pytest plugins/pgcache/ -v -m pgcache
```

See [plugins/pgcache/README.md](../plugins/pgcache/README.md) for setup requirements.
