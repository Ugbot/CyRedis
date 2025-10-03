# CyRedis Testing Documentation

Comprehensive guide for testing the CyRedis high-performance Redis client library.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Test Organization](#test-organization)
- [Running Tests](#running-tests)
- [Docker Testing Environment](#docker-testing-environment)
- [CI/CD Integration](#cicd-integration)
- [Code Coverage](#code-coverage)
- [Performance Testing](#performance-testing)
- [Writing Tests](#writing-tests)
- [Troubleshooting](#troubleshooting)

## Overview

### Testing Philosophy

CyRedis employs a comprehensive testing strategy that ensures:

- **Correctness**: All Redis operations work as expected through the Cython/hiredis backend
- **Performance**: Benchmarks validate the performance claims (3-5x faster operations)
- **Reliability**: Integration tests verify real-world usage patterns
- **Compatibility**: Tests cover standalone Redis, clusters, and sentinel configurations

### Testing Stack

- **pytest**: Primary test framework (v6.0+)
- **pytest-asyncio**: For async operation testing
- **pytest-cov**: Code coverage reporting
- **pytest-benchmark**: Performance benchmarking (optional)
- **Docker**: Isolated test environments with docker-compose
- **uvloop**: Enhanced async performance testing

## Quick Start

### Prerequisites

```bash
# Install test dependencies using UV (preferred)
uv pip install -e ".[test]"

# Or using standard pip
pip install -e ".[test]"

# Start local Redis (if not using Docker)
redis-server --daemonize yes
```

### Run All Tests

```bash
# From project root
pytest

# With verbose output
pytest -v

# Run specific test category
pytest -m unit          # Unit tests only
pytest -m integration   # Integration tests only
pytest -m benchmark     # Performance benchmarks
```

## Test Organization

### Directory Structure

```
tests/
├── conftest.py              # Shared fixtures for all tests
├── pytest.ini               # Pytest configuration
├── docker-compose.test.yml  # Docker test environment
├── unit/                    # Unit tests (no external services)
│   ├── conftest.py
│   ├── test_redis_core.py
│   ├── test_messaging.py
│   ├── test_protocol.py
│   ├── test_connection_pool.py
│   ├── test_script_manager.py
│   ├── test_functions.py
│   ├── test_shared_dict.py
│   ├── test_distributed.py
│   └── test_async_core.py
├── integration/             # Integration tests (require services)
│   ├── conftest.py
│   ├── test_basic_operations.py
│   ├── test_async_operations.py
│   ├── test_messaging_flows.py
│   ├── test_distributed_locks.py
│   └── test_concurrent_access.py
├── apps/                    # Application-level tests
└── docker/                  # Docker configurations
    ├── docker-compose.yml
    ├── redis-cluster/
    └── redis-sentinel/
```

### Test Categories

#### Unit Tests (`tests/unit/`)
- Test individual Cython modules in isolation
- Mock external dependencies where needed
- Fast execution, no external services required
- Marker: `@pytest.mark.unit`

#### Integration Tests (`tests/integration/`)
- Test complete workflows with real Redis
- Verify cross-module interactions
- Require running Redis instance
- Markers: `@pytest.mark.integration`, `@pytest.mark.requires_redis`

#### Performance Tests
- Benchmark critical operations
- Compare against baseline metrics
- Validate performance claims
- Marker: `@pytest.mark.benchmark`

#### Specialized Tests
- **Cluster**: Redis cluster functionality (`@pytest.mark.cluster`)
- **Sentinel**: Sentinel failover testing (`@pytest.mark.sentinel`)
- **PGCache**: PostgreSQL integration (`@pytest.mark.pgcache`)
- **Async**: Async operations (`@pytest.mark.async_test`)

## Running Tests

### Basic Commands

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=cy_redis --cov-report=html

# Run specific test file
pytest tests/unit/test_redis_core.py

# Run specific test class or function
pytest tests/unit/test_redis_core.py::TestRedisCore::test_set_get
pytest tests/integration/test_messaging_flows.py::test_queue_reliability

# Run tests matching pattern
pytest -k "test_async"
pytest -k "not slow"
```

### Using Markers

```bash
# Unit tests only (fast, no external services)
pytest -m unit

# Integration tests only (requires Redis)
pytest -m integration

# Skip slow tests
pytest -m "not slow"

# Run cluster tests
pytest -m cluster

# Run all except Docker-dependent tests
pytest -m "not docker"

# Benchmark tests only
pytest -m benchmark

# PostgreSQL cache tests
pytest -m pgcache
```

### Parallel Execution

```bash
# Install pytest-xdist
uv pip install pytest-xdist

# Run tests in parallel
pytest -n auto              # Auto-detect CPU count
pytest -n 4                 # Use 4 workers
```

### Verbose Output Options

```bash
# Show print statements
pytest -s

# Verbose test names
pytest -v

# Very verbose with local variables
pytest -vv --showlocals

# Show full tracebacks
pytest --tb=long

# Stop on first failure
pytest -x

# Show summary of all test types
pytest -ra
```

## Docker Testing Environment

### Starting Test Services

```bash
# Start all test services (Redis, PostgreSQL, Cluster, Sentinel)
docker compose -f tests/docker-compose.test.yml up -d

# Start specific service
docker compose -f tests/docker-compose.test.yml up -d redis
docker compose -f tests/docker-compose.test.yml up -d postgres

# Check service health
docker compose -f tests/docker-compose.test.yml ps

# View logs
docker compose -f tests/docker-compose.test.yml logs -f redis
```

### Available Services

The Docker test environment provides:

- **redis** (port 6379): Standalone Redis for basic testing
- **redis-auth** (port 6380): Redis with password authentication
- **postgres** (port 5432): PostgreSQL for pgcache tests
- **redis-cluster-{1,2,3}** (ports 7000-7002): 3-node Redis cluster
- **redis-master** (port 6400): Sentinel master
- **redis-replica-{1,2}** (ports 6401-6402): Sentinel replicas
- **redis-sentinel-{1,2,3}** (ports 26379-26381): Sentinel instances

### Running Tests with Docker

```bash
# Start services and run tests
docker compose -f tests/docker-compose.test.yml up -d
pytest -m integration
docker compose -f tests/docker-compose.test.yml down

# One-liner for CI
docker compose -f tests/docker-compose.test.yml up -d && \
  sleep 5 && \
  pytest -m integration && \
  docker compose -f tests/docker-compose.test.yml down
```

### Environment Variables

```bash
# Configure Redis connection
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_PASSWORD=testpassword
export REDIS_TEST_DB=15

# Configure cluster
export REDIS_CLUSTER_NODES="localhost:7000,localhost:7001,localhost:7002"

# Configure sentinel
export REDIS_SENTINEL_NODES="localhost:26379,localhost:26380,localhost:26381"
export REDIS_SENTINEL_MASTER=mymaster

# Configure PostgreSQL
export POSTGRES_TEST_DSN="postgresql://postgres:postgres@localhost:5432/test_db"

# Run tests with custom config
pytest
```

## CI/CD Integration

### GitHub Actions Example

Create `.github/workflows/test.yml`:

```yaml
name: Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      redis:
        image: redis:7-alpine
        ports:
          - 6379:6379
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

      postgres:
        image: postgres:15-alpine
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test_db
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
    - uses: actions/checkout@v3
      with:
        submodules: recursive

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Install UV
      run: |
        curl -LsSf https://astral.sh/uv/install.sh | sh
        echo "$HOME/.cargo/bin" >> $GITHUB_PATH

    - name: Install dependencies
      run: |
        uv pip install -e ".[test]"

    - name: Build Cython extensions
      run: |
        bash build_optimized.sh

    - name: Run unit tests
      run: |
        pytest -m unit --cov=cy_redis --cov-report=xml

    - name: Run integration tests
      run: |
        pytest -m integration --cov=cy_redis --cov-append --cov-report=xml

    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
        fail_ci_if_error: true
```

### Pre-commit Hooks

Install pre-commit hooks in `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: pytest-unit
        name: pytest-unit
        entry: pytest -m unit
        language: system
        pass_filenames: false
        always_run: true
```

## Code Coverage

### Generating Coverage Reports

```bash
# HTML report (interactive)
pytest --cov=cy_redis --cov-report=html
open htmlcov/index.html

# Terminal report
pytest --cov=cy_redis --cov-report=term-missing

# XML report (for CI)
pytest --cov=cy_redis --cov-report=xml

# Multiple formats
pytest --cov=cy_redis \
  --cov-report=html \
  --cov-report=term-missing \
  --cov-report=xml
```

### Coverage Configuration

Coverage settings are in `tests/pytest.ini`:

```ini
[coverage:run]
source = cy_redis
branch = true
omit =
    */tests/*
    */cyredis_venv/*

[coverage:report]
precision = 2
show_missing = true
exclude_lines =
    pragma: no cover
    def __repr__
    raise NotImplementedError
    if __name__ == .__main__.:
```

### Coverage Goals

- **Core functionality**: >95% coverage
- **Error paths**: All exceptions tested
- **Critical paths**: 100% coverage
- **Overall target**: >90% coverage

## Performance Testing

### Running Benchmarks

```bash
# Install benchmark plugin
uv pip install pytest-benchmark

# Run all benchmarks
pytest -m benchmark

# Save baseline
pytest -m benchmark --benchmark-save=baseline

# Compare against baseline
pytest -m benchmark --benchmark-compare=baseline

# Generate histogram
pytest -m benchmark --benchmark-histogram
```

### Writing Benchmark Tests

```python
import pytest

@pytest.mark.benchmark
def test_set_performance(benchmark, redis_client):
    """Benchmark SET operation."""
    def set_operation():
        redis_client.set("bench:key", "value")

    result = benchmark(set_operation)

    # Assert performance requirement (e.g., < 10 microseconds)
    assert result.stats.mean < 0.00001  # 10μs

@pytest.mark.benchmark
def test_pipeline_performance(benchmark, redis_client):
    """Benchmark pipelined operations."""
    def pipeline_ops():
        pipe = redis_client.pipeline()
        for i in range(100):
            pipe.set(f"bench:key:{i}", f"value_{i}")
        pipe.execute()

    benchmark(pipeline_ops)
```

### Performance Metrics

Track key performance indicators:

| Operation | Target | Current |
|-----------|--------|---------|
| SET/GET | < 2μs | TBD |
| Pipeline (100 ops) | < 500μs | TBD |
| Pub/Sub message | < 5μs | TBD |
| Stream XADD | < 3μs | TBD |

## Writing Tests

### Test Structure Template

```python
"""
Test module for <feature> functionality.
"""
import pytest
from cy_redis import <module>


class Test<Feature>:
    """Test suite for <feature>."""

    @pytest.mark.unit
    def test_basic_operation(self, redis_client):
        """Test basic <feature> operation."""
        # Arrange
        key = "test:feature"
        value = "test_value"

        # Act
        redis_client.set(key, value)
        result = redis_client.get(key)

        # Assert
        assert result == value

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_async_operation(self, async_redis_client):
        """Test async <feature> operation."""
        key = "test:async"
        value = "async_value"

        await async_redis_client.set(key, value)
        result = await async_redis_client.get(key)

        assert result == value

    @pytest.mark.benchmark
    def test_performance(self, benchmark, redis_client):
        """Benchmark <feature> performance."""
        def operation():
            redis_client.some_operation()

        benchmark(operation)
```

### Using Fixtures

Common fixtures from `tests/conftest.py`:

```python
def test_with_fixtures(
    redis_client,          # Redis client instance
    async_redis_client,    # Async Redis client
    redis_cluster_client,  # Cluster client
    random_key,            # Generate unique keys
    sample_data,           # Sample test data
    perf_timer,            # Performance timer
    test_namespace,        # Unique namespace
):
    """Example using multiple fixtures."""
    key = random_key("mytest")

    with perf_timer("operation") as timer:
        redis_client.set(key, sample_data["string"])

    assert timer.elapsed < 0.001  # < 1ms
```

### Best Practices

1. **Test Isolation**: Each test should be independent
   ```python
   @pytest.fixture
   def isolated_client(redis_client):
       """Provide isolated client with cleanup."""
       namespace = f"test:{int(time.time())}"
       yield redis_client
       # Cleanup namespace keys
       keys = redis_client.keys(f"{namespace}:*")
       if keys:
           redis_client.delete(*keys)
   ```

2. **Async Testing**: Use proper async fixtures
   ```python
   @pytest.mark.asyncio
   async def test_async(async_redis_client):
       result = await async_redis_client.ping()
       assert result == b"PONG"
   ```

3. **Error Testing**: Verify error conditions
   ```python
   def test_error_handling(redis_client):
       with pytest.raises(ValueError):
           redis_client.invalid_operation()
   ```

4. **Parametrize Tests**: Test multiple inputs
   ```python
   @pytest.mark.parametrize("value", [
       "string",
       b"bytes",
       123,
       {"key": "dict"},
   ])
   def test_set_types(redis_client, random_key, value):
       key = random_key()
       redis_client.set(key, value)
       assert redis_client.get(key) == value
   ```

## Troubleshooting

### Common Issues

#### 1. Redis Connection Failed

**Problem**: `ConnectionError: Error connecting to Redis`

**Solutions**:
```bash
# Check if Redis is running
redis-cli ping

# Start Redis
redis-server --daemonize yes

# Or use Docker
docker compose -f tests/docker-compose.test.yml up -d redis

# Check connection settings
export REDIS_HOST=localhost
export REDIS_PORT=6379
```

#### 2. Import Errors

**Problem**: `ImportError: No module named 'cy_redis'`

**Solutions**:
```bash
# Build Cython extensions
bash build_optimized.sh

# Or standard build
python setup.py build_ext --inplace

# Verify build
python -c "from cy_redis import HighPerformanceRedis; print('OK')"
```

#### 3. Test Failures in CI

**Problem**: Tests pass locally but fail in CI

**Solutions**:
```yaml
# Ensure services are ready in CI
- name: Wait for Redis
  run: |
    timeout 30 bash -c 'until redis-cli ping; do sleep 1; done'

# Use correct environment variables
env:
  REDIS_HOST: localhost
  REDIS_PORT: 6379
```

#### 4. Coverage Not Generated

**Problem**: Coverage report is empty

**Solutions**:
```bash
# Ensure pytest-cov is installed
uv pip install pytest-cov

# Run with explicit coverage
pytest --cov=cy_redis --cov-report=html

# Check .coveragerc configuration
cat tests/pytest.ini
```

#### 5. Slow Tests

**Problem**: Test suite takes too long

**Solutions**:
```bash
# Run only unit tests (fast)
pytest -m unit

# Skip slow tests
pytest -m "not slow"

# Use parallel execution
pytest -n auto

# Profile tests
pytest --durations=10
```

### Debug Mode

```bash
# Run with debug output
pytest -vv --showlocals --tb=long

# Drop into debugger on failure
pytest --pdb

# Debug specific test
pytest tests/unit/test_redis_core.py::test_set -vv --pdb

# Enable logging
pytest --log-cli-level=DEBUG
```

### Getting Help

1. **Check test output**: Read error messages carefully
2. **Review fixtures**: Ensure required services are available
3. **Check environment**: Verify environment variables
4. **Review logs**: Check Docker/service logs for issues
5. **Consult docs**: See `tests/README.md` for detailed test structure

## Additional Resources

- **Test Structure**: See `tests/README.md`
- **Examples**: Check `examples/` directory
- **CI Examples**: See `.github/workflows/` (if present)
- **Docker Setup**: Review `tests/docker-compose.test.yml`
- **Fixtures Reference**: See `tests/conftest.py`

---

**Note**: This testing infrastructure is designed to ensure CyRedis maintains its performance claims (3-5x faster than standard Redis clients) while maintaining 100% compatibility with Redis operations.
