# CyRedis Unit Tests

This directory contains comprehensive unit tests for the CyRedis library.

## Test Structure

The unit tests are organized by module:

- `test_redis_core.py` - Tests for basic Redis operations (GET, SET, DELETE, etc.)
- `test_async_core.py` - Tests for async Redis operations
- `test_messaging.py` - Tests for messaging primitives (queues, locks, etc.)
- `test_distributed.py` - Tests for distributed features (locks, semaphores)
- `test_connection_pool.py` - Tests for connection pooling and health checks
- `test_protocol.py` - Tests for RESP2/3 protocol support
- `test_functions.py` - Tests for Redis Functions
- `test_script_manager.py` - Tests for Lua script management
- `test_shared_dict.py` - Tests for shared dictionary

## Running Tests

### Run all unit tests:
```bash
pytest tests/unit/
```

### Run specific test file:
```bash
pytest tests/unit/test_redis_core.py
```

### Run specific test class:
```bash
pytest tests/unit/test_redis_core.py::TestCyRedisClient
```

### Run specific test:
```bash
pytest tests/unit/test_redis_core.py::TestCyRedisClient::test_set_get
```

### Run with verbose output:
```bash
pytest tests/unit/ -v
```

### Run with coverage:
```bash
pytest tests/unit/ --cov=cy_redis --cov-report=html
```

### Run tests in parallel:
```bash
pytest tests/unit/ -n auto
```

### Skip slow tests:
```bash
pytest tests/unit/ -m "not slow"
```

## Requirements

### Redis Server
Most tests require a running Redis server on localhost:6379. You can start Redis using Docker:

```bash
docker run -d -p 6379:6379 redis:latest
```

### Python Dependencies
Install test dependencies:

```bash
pip install pytest pytest-asyncio pytest-cov pytest-xdist
```

## Test Categories

Tests are marked with the following markers:

- `@pytest.mark.redis` - Requires Redis server
- `@pytest.mark.slow` - Slow-running tests
- `@pytest.mark.asyncio` - Async tests requiring asyncio support

## Writing New Tests

When adding new tests:

1. Follow the existing test structure and naming conventions
2. Use fixtures for common setup/teardown
3. Test both success and error cases
4. Include edge cases and boundary conditions
5. Add docstrings to test functions
6. Clean up resources in fixtures or teardown

## Test Fixtures

Common fixtures are defined in `conftest.py`:

- `redis_client` - A CyRedisClient instance
- `redis_available` - Checks if Redis is available
- Test environment setup/teardown

## Code Coverage

Aim for high code coverage:

- Line coverage: > 80%
- Branch coverage: > 70%
- Critical paths: 100%

Generate coverage report:
```bash
pytest tests/unit/ --cov=cy_redis --cov-report=html
open htmlcov/index.html
```

## Debugging Tests

Run with debugging output:
```bash
pytest tests/unit/ -v -s --log-cli-level=DEBUG
```

Run single test with pdb:
```bash
pytest tests/unit/test_redis_core.py::test_name --pdb
```

## Continuous Integration

These tests are run automatically in CI/CD pipelines:
- On every commit
- Before merging pull requests
- On release branches

## Notes

- Tests are designed to be independent and can run in any order
- Each test cleans up its own resources
- Tests use unique keys/identifiers to avoid collisions
- Async tests use pytest-asyncio plugin
- Some tests may be skipped if Redis is not available
