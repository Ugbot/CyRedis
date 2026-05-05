"""
Pytest configuration for unit tests

This file contains shared fixtures and configuration for all unit tests.
"""
import pytest
import os
import sys
from typing import Any, List


def pytest_configure(config: Any) -> None:
    """Configure pytest"""
    # Add custom markers
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "redis: marks tests that require Redis server"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )


@pytest.fixture(scope="session")
def redis_available() -> bool:
    """Check if Redis is available for testing"""
    try:
        # Prefer the compiled core client; fallback to top-level if present
        try:
            from cy_redis.core.cy_redis_client import CyRedisClient
        except ImportError:
            from cy_redis.core.cy_redis_client import CyRedisClient
        if CyRedisClient is None:
            return False
        client = CyRedisClient(host="localhost", port=6379)
        client.set("pytest_test", "ok")
        result = client.get("pytest_test")
        client.delete("pytest_test")
        return result == "ok"
    except Exception:
        return False


@pytest.fixture(autouse=True)
def check_redis(request: Any, redis_available: bool) -> None:
    """Automatically skip tests that require Redis if it's not available"""
    if request.node.get_closest_marker('redis'):
        if not redis_available:
            pytest.skip('Redis not available')


def pytest_collection_modifyitems(config: Any, items: List[Any]) -> None:
    """Modify test collection"""
    for item in items:
        # Automatically mark tests that use redis_client fixture
        if 'redis_client' in item.fixturenames:
            item.add_marker(pytest.mark.redis)


# Environment setup
@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment"""
    # Set test environment variables
    os.environ['CYREDIS_TEST_MODE'] = '1'

    yield

    # Cleanup
    if 'CYREDIS_TEST_MODE' in os.environ:
        del os.environ['CYREDIS_TEST_MODE']
