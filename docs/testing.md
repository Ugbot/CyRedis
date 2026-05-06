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

Unit tests use in-memory mocks — no Redis process required.

```python
# MockRedisClient: simulates the async interface with asyncio.Queue
# Available in tests/unit/conftest.py

class MockRedisClient:
    async def publish_async(self, channel, message): ...
    async def xadd_async(self, stream, data, message_id="*"): ...
    async def xread_async(self, streams, count, block): ...
    async def hset_async(self, key, ...): ...
    async def hgetall_async(self, key): ...
    async def set_async(self, key, value): ...
    async def get_async(self, key): ...
    async def delete_async(self, key): ...
    async def execute_command_async(self, *args): ...
```

```python
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

## CI

GitHub Actions runs two workflows:

- **Linux** (`ubuntu-latest`): Redis 7-alpine on port 6379, Valkey 8-alpine on port 6380
- **macOS** (`macos-latest`): same setup via service containers

```bash
# Reproduce CI locally
uv run pytest tests/ -m "not skip_ci" --timeout=60
```

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
