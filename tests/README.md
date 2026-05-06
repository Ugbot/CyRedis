# Tests

← [README](../README.md) | [Testing guide](../docs/testing.md)

See [docs/testing.md](../docs/testing.md) for the full guide — how to run tests, fixture reference, CI setup, and contributing guidelines.

## Quick commands

```bash
# Unit tests (no Redis required)
uv run pytest tests/unit/ -v

# Integration tests (requires Redis on localhost:6379)
uv run pytest tests/integration/ -v

# Specific file
uv run pytest tests/unit/test_channels.py -v

# With coverage
uv run pytest tests/unit/ --cov=cy_redis --cov-report=term-missing
```

## Layout

```
tests/
  conftest.py          — shared fixtures
  unit/
    conftest.py        — MockRedisClient, MockWebSocket
    test_channels.py
    test_messaging.py
    test_protocol.py
    ...
  integration/
    conftest.py        — live Redis fixtures
    test_worker_coordination.py
    ...
```
