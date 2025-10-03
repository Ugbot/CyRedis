"""
CyRedis Tests
Test suite for CyRedis functionality and performance validation.

This package contains:
- Unit tests: Fast, isolated tests of individual components
- Integration tests: Tests that require external services (Redis, PostgreSQL, etc.)
- Performance tests: Benchmarks and performance validation
"""

__version__ = "0.1.0"

# Test configuration defaults
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_TEST_DB = 15

# Test constants
TEST_KEY_PREFIX = "test:"
TEST_TIMEOUT = 30  # seconds
MAX_TEST_RETRIES = 3
