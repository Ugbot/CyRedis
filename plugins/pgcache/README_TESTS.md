# PGCACHE Test Suite

This comprehensive test suite validates all the enhanced features of the pgcache Redis module.

## 🚀 Quick Start

### 1. Setup Test Environment

```bash
# Run the setup script to configure PostgreSQL and load the module
python3 setup_test_environment.py
```

### 2. Run All Tests

```bash
# Run complete test suite
python3 run_tests.py

# Or run specific test types
python3 run_tests.py --unit-only          # Unit tests only
python3 run_tests.py --integration-only   # Integration tests only
python3 run_tests.py --verbose             # Verbose output
```

### 3. Run Individual Tests

```bash
# Basic functionality tests
python3 test_pgcache.py

# Full integration tests (requires PostgreSQL)
python3 test_integration.py
```

## 📋 Test Coverage

### Unit Tests (`test_pgcache.py`)

Tests core functionality without external dependencies:

- ✅ **Basic Commands**: Status, configuration validation
- ✅ **Connection Pooling**: Pool size configuration and management
- ✅ **Watch Management**: Add, remove, list watch entries
- ✅ **Redis Streams**: Stream info and basic operations
- ✅ **Performance Monitoring**: Metrics collection and reporting
- ✅ **Transaction Support**: Command availability (without PG setup)

### Integration Tests (`test_integration.py`)

Full integration tests with PostgreSQL:

- ✅ **Cache Operations**: Read/write with real PostgreSQL data
- ✅ **Cache Invalidation**: Notification-based invalidation testing
- ✅ **Watch Forwarding**: Real-time event forwarding to streams
- ✅ **Performance Metrics**: Live performance monitoring
- ✅ **Transaction Support**: Database transaction handling

### Performance Tests (built into `run_tests.py`)

- ⚡ **Cache Performance**: Operations per second measurement
- ⚡ **Throughput Testing**: Concurrent operation handling

## 🔧 Prerequisites

### Required Software

1. **Redis** with pgcache module loaded
2. **PostgreSQL** (for integration tests)
3. **Python 3.7+** with required packages

### Required Python Packages

```bash
pip install redis psycopg2-binary
```

### PostgreSQL Setup

For full integration tests, ensure:

1. PostgreSQL is running on localhost:5432
2. A `postgres` user exists (or modify connection settings)
3. The test database can be created and dropped

## 📊 Test Results

### Sample Output

```
🚀 Starting comprehensive pgcache test suite...
[14:23:15] INFO: Running unit tests...
[14:23:16] INFO: ✅ Unit tests completed successfully
[14:23:16] INFO: Running integration tests...
[14:23:18] INFO: ✅ Integration tests completed successfully
[14:23:18] INFO: Running performance tests...
[14:23:19] INFO: ✅ Performance test: 1547.23 ops/sec (0.065s for 100 ops)

📊 SUMMARY:
   Total Tests: 3
   Passed: 3
   Failed: 0
   Errors: 0
   Duration: 4.12s

🎉 All tests passed! pgcache is working correctly.
```

### Test Result Files

- **Live output**: Console display during execution
- **Detailed JSON**: `test_results_YYYYMMDD_HHMMSS.json` (auto-generated)
- **Individual results**: `test_results.json` (from individual test files)

## 🛠️ Troubleshooting

### Common Issues

#### "pgcache module not available"
```bash
# Ensure the module is built and loaded
make
redis-cli MODULE LOAD ./pgcache.so
```

#### "PostgreSQL connection failed"
```bash
# Check PostgreSQL status
sudo systemctl status postgresql
# Or start PostgreSQL
sudo systemctl start postgresql
```

#### "Module commands not found"
```bash
# Verify module is loaded in Redis
redis-cli MODULE LIST
# Should show pgcache in the list
```

### Manual Testing

You can also test individual commands manually:

```bash
# Check module status
redis-cli PGCACHE.STATUS

# Configure connection pool
redis-cli PGCACHE.SETUP.CONNECTIONPOOL 5

# Add a watch
redis-cli PGCACHE.WATCH.ADD users id:* user_updates user_stream

# Check metrics
redis-cli PGCACHE.METRICS
```

## 🔍 Test Details

### Unit Test Coverage

| Feature | Test Function | Description |
|---------|---------------|-------------|
| Status | `test_basic_functionality` | Module status and configuration |
| Pool | `test_connection_pooling` | Connection pool management |
| Watches | `test_watch_management` | Watch add/remove/list |
| Streams | `test_redis_streams` | Stream operations and info |
| Metrics | `test_performance_monitoring` | Performance monitoring |
| Transactions | `test_transaction_support` | Transaction command availability |

### Integration Test Coverage

| Feature | Test Function | Description |
|---------|---------------|-------------|
| Cache | `test_cache_operations` | Real PostgreSQL cache operations |
| Invalidation | `test_cache_invalidation` | Live cache invalidation testing |
| Forwarding | `test_watch_forwarding` | Real-time event forwarding |
| Metrics | `test_performance_metrics` | Live performance monitoring |
| Transactions | `test_transaction_support` | Database transaction handling |

## 📈 Performance Benchmarks

The test suite includes basic performance benchmarks:

- **Cache Operations**: Measures read/write throughput
- **Concurrent Access**: Tests connection pool performance
- **Memory Usage**: Validates efficient resource utilization

## 🤝 Contributing

To add new tests:

1. Add test function to appropriate test file
2. Follow naming convention: `test_feature_name`
3. Use `self.assert_*` methods for validation
4. Add documentation in this README

## 📝 Configuration

Test configuration can be modified in:

- `test_config.json` - Generated by setup script
- Individual test files - Hardcoded connection settings
- Environment variables - For custom test environments

## 🔒 Security Notes

- Tests create/drop test databases automatically
- Test data is isolated to `pgcache_test` database
- No production data is affected
- Credentials should be configured for your environment
