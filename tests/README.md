# CyRedis Tests

Test suite for CyRedis functionality, performance validation, and regression testing.

## 🧪 Test Structure

### Integration Tests
- **`test_pgcache_integration.py`** - PostgreSQL read-through cache integration tests

### Planned Test Categories
- **Unit Tests**: Core functionality testing
- **Performance Tests**: Benchmarking and optimization validation
- **Integration Tests**: External service integrations
- **Stress Tests**: High-load and edge case testing
- **Cluster Tests**: Redis/Valkey cluster compatibility

## 🚀 Running Tests

### Prerequisites
```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-benchmark

# Start Redis/Valkey for integration tests
redis-server --daemonize yes
# or
valkey-server --daemonize yes
```

### Run All Tests
```bash
# From project root
pytest tests/

# With verbose output
pytest tests/ -v

# With coverage
pytest tests/ --cov=cy_redis --cov-report=html
```

### Run Specific Tests
```bash
# Run integration tests only
pytest tests/test_pgcache_integration.py

# Run with specific markers
pytest tests/ -m "integration"

# Run performance benchmarks
pytest tests/ -m "benchmark"
```

## 📊 Test Categories

### 🔬 Unit Tests
```python
def test_basic_operations():
    client = RedisClient()
    client.set("test", "value")
    assert client.get("test") == "value"
```

### 🔗 Integration Tests
```python
def test_pgcache_integration():
    # Test PostgreSQL cache integration
    cache = PGCacheManager(redis_client, pg_config)
    result = cache.get("SELECT * FROM users WHERE id = 1")
    assert result is not None
```

### ⚡ Performance Tests
```python
def test_messagepack_performance(benchmark):
    @benchmark
    def serialize_data():
        return serialize_game_data({"key": "value", "data": [1,2,3,4,5]})
```

### 🌐 Cluster Tests
```python
def test_cluster_operations():
    # Test hash tag safety and cluster operations
    client = RedisClient(cluster_urls=["redis://node1", "redis://node2"])
    # Zone operations should stay within hash slots
```

## 🛠️ Testing Infrastructure

### Fixtures
```python
@pytest.fixture
def redis_client():
    client = RedisClient()
    yield client
    client.flushall()  # Cleanup

@pytest.fixture
def game_engine():
    engine = GameEngine()
    engine.load_functions()
    yield engine
```

### Markers
```python
@pytest.mark.integration
def test_external_service():
    # Requires external services

@pytest.mark.cluster
def test_cluster_features():
    # Requires Redis cluster

@pytest.mark.benchmark
def test_performance():
    # Performance benchmarking
```

## 📈 Performance Benchmarking

### Setup Benchmarks
```python
import pytest_benchmark

def test_serialization_performance(benchmark):
    test_data = {"entities": [{"id": i, "x": i*10, "y": i*20} for i in range(1000)]}

    @benchmark
    def msgpack_serialization():
        return serialize_game_data(test_data)

    @benchmark
    def json_serialization():
        return json.dumps(test_data)
```

### Benchmark Results
```
Name                          Time        Compare
msgpack_serialization      15.3μs         1.0x
json_serialization         145.2μs        9.5x
```

## 🎯 Test Coverage Goals

- **Core Functionality**: >95% coverage
- **Error Handling**: All error paths tested
- **Performance**: Benchmarks for all critical paths
- **Integration**: External service compatibility
- **Cluster**: All cluster operations validated

## 🔧 CI/CD Integration

### GitHub Actions
```yaml
- name: Run Tests
  run: |
    pytest tests/ --cov=cy_redis --cov-report=xml

- name: Performance Regression
  run: |
    pytest tests/ -m benchmark --benchmark-save=results
    # Compare against baseline
```

### Pre-commit Hooks
```yaml
repos:
  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: pytest
        language: system
        pass_filenames: false
        args: [tests/]
```

## 🤝 Contributing Tests

### Adding New Tests
1. Create test file: `tests/test_<feature>.py`
2. Use descriptive test names: `test_<action>_<condition>_<result>`
3. Include docstrings explaining test purpose
4. Add appropriate markers and fixtures

### Test File Template
```python
"""
Test <Feature> functionality
"""
import pytest
from cy_redis import RedisClient


class TestFeature:
    """Test suite for <Feature>"""

    @pytest.fixture
    def client(self):
        """Redis client fixture"""
        client = RedisClient()
        yield client
        client.flushall()

    def test_basic_functionality(self, client):
        """Test basic <feature> operations"""
        # Test implementation
        pass

    @pytest.mark.asyncio
    async def test_async_operations(self, client):
        """Test async <feature> operations"""
        # Async test implementation
        pass

    @pytest.mark.benchmark
    def test_performance(self, benchmark, client):
        """Performance benchmark for <feature>"""
        def operation():
            # Operation to benchmark
            pass

        benchmark(operation)
```

## 📊 Test Metrics

### Coverage Report
```bash
pytest tests/ --cov=cy_redis --cov-report=html
# Open htmlcov/index.html
```

### Performance Tracking
```bash
pytest tests/ -m benchmark --benchmark-histogram
# Generates performance history
```

## 🔍 Debugging Tests

### Verbose Output
```bash
pytest tests/ -v -s --tb=long
```

### Debug Specific Test
```bash
pytest tests/test_specific.py::TestClass::test_method -xvs
```

### PDB Integration
```python
def test_debug():
    import pdb; pdb.set_trace()
    # Test code
```

## 📞 Support

- Test failures: Check Redis/Valkey connectivity
- Performance issues: Compare against baseline benchmarks
- Integration problems: Verify external service configuration
- CI/CD issues: Check GitHub Actions logs
