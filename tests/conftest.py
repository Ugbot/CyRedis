"""
Shared pytest fixtures for CyRedis test suite.

This module provides comprehensive fixtures for testing Redis functionality
including standalone, cluster, sentinel configurations, async support,
performance measurements, and Docker service management.
"""

import os
import sys
import time
import asyncio
import logging
from typing import Generator, Optional, Dict, Any, List
from contextlib import contextmanager
from pathlib import Path

import pytest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Path and Environment Setup
# ============================================================================

@pytest.fixture(scope="session", autouse=True)
def setup_python_path():
    """Add project root to Python path for imports."""
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    yield
    sys.path.remove(str(project_root))


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).parent.parent


# ============================================================================
# Redis Connection Configuration
# ============================================================================

@pytest.fixture(scope="session")
def redis_host() -> str:
    """Redis host from environment or default."""
    return os.getenv("REDIS_HOST", "localhost")


@pytest.fixture(scope="session")
def redis_port() -> int:
    """Redis port from environment or default."""
    return int(os.getenv("REDIS_PORT", "6379"))


@pytest.fixture(scope="session")
def redis_db() -> int:
    """Redis database number for tests."""
    return int(os.getenv("REDIS_TEST_DB", "15"))


@pytest.fixture(scope="session")
def redis_password() -> Optional[str]:
    """Redis password from environment."""
    return os.getenv("REDIS_PASSWORD")


@pytest.fixture(scope="session")
def redis_cluster_nodes() -> List[Dict[str, Any]]:
    """Redis cluster node configuration."""
    nodes_str = os.getenv("REDIS_CLUSTER_NODES", "localhost:7000,localhost:7001,localhost:7002")
    nodes = []
    for node in nodes_str.split(","):
        host, port = node.strip().split(":")
        nodes.append({"host": host, "port": int(port)})
    return nodes


@pytest.fixture(scope="session")
def redis_sentinel_nodes() -> List[Dict[str, Any]]:
    """Redis sentinel node configuration."""
    nodes_str = os.getenv("REDIS_SENTINEL_NODES", "localhost:26379,localhost:26380,localhost:26381")
    nodes = []
    for node in nodes_str.split(","):
        host, port = node.strip().split(":")
        nodes.append({"host": host, "port": int(port)})
    return nodes


@pytest.fixture(scope="session")
def redis_sentinel_master() -> str:
    """Redis sentinel master name."""
    return os.getenv("REDIS_SENTINEL_MASTER", "mymaster")


# ============================================================================
# Service Availability Checks
# ============================================================================

def check_redis_available(host: str, port: int, password: Optional[str] = None) -> bool:
    """Check if Redis is available at the given host and port."""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.warning(f"Redis availability check failed: {e}")
        return False


def check_docker_available() -> bool:
    """Check if Docker is available."""
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception as e:
        logger.warning(f"Docker availability check failed: {e}")
        return False


@pytest.fixture(scope="session")
def redis_available(redis_host, redis_port) -> bool:
    """Check if Redis service is available."""
    return check_redis_available(redis_host, redis_port)


@pytest.fixture(scope="session")
def docker_available() -> bool:
    """Check if Docker is available."""
    return check_docker_available()


# ============================================================================
# Skip Markers Based on Service Availability
# ============================================================================

def skip_if_no_redis(redis_available: bool):
    """Skip test if Redis is not available."""
    if not redis_available:
        pytest.skip("Redis service not available")


def skip_if_no_docker(docker_available: bool):
    """Skip test if Docker is not available."""
    if not docker_available:
        pytest.skip("Docker service not available")


# ============================================================================
# Redis Client Fixtures
# ============================================================================

@pytest.fixture
def redis_client(redis_host, redis_port, redis_db, redis_password, redis_available):
    """
    Provide a Redis client instance for testing.
    Automatically cleans up test keys after use.
    """
    skip_if_no_redis(redis_available)

    try:
        # Try to import CyRedis client
        from cy_redis.cy_redis_client import CyRedisClient
        client = CyRedisClient(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password
        )
    except ImportError:
        # Fallback to redis-py if CyRedis not built
        try:
            import redis
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True
            )
        except ImportError:
            pytest.skip("Neither CyRedis nor redis-py available")

    # Track keys created during test for cleanup
    keys_before = set()
    try:
        keys_before = set(client.keys("test:*") or [])
    except Exception:
        pass

    yield client

    # Cleanup: remove keys created during test
    try:
        keys_after = set(client.keys("test:*") or [])
        keys_to_delete = keys_after - keys_before
        if keys_to_delete:
            client.delete(*keys_to_delete)
    except Exception as e:
        logger.warning(f"Failed to cleanup test keys: {e}")

    # Close connection
    try:
        if hasattr(client, 'close'):
            client.close()
        elif hasattr(client, 'connection_pool'):
            client.connection_pool.disconnect()
    except Exception:
        pass


@pytest.fixture
async def async_redis_client(redis_host, redis_port, redis_db, redis_password, redis_available):
    """
    Provide an async Redis client instance for testing.
    Automatically cleans up test keys after use.
    """
    skip_if_no_redis(redis_available)

    try:
        # Try to import async CyRedis client
        from cy_redis.async_core import AsyncRedisClient
        client = AsyncRedisClient(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password
        )
        await client.connect()
    except ImportError:
        # Fallback to redis-py async
        try:
            import redis.asyncio as aioredis
            client = await aioredis.from_url(
                f"redis://{redis_host}:{redis_port}/{redis_db}",
                password=redis_password,
                decode_responses=True
            )
        except ImportError:
            pytest.skip("Neither async CyRedis nor redis-py available")

    # Track keys created during test for cleanup
    keys_before = set()
    try:
        keys_before = set(await client.keys("test:*") or [])
    except Exception:
        pass

    yield client

    # Cleanup: remove keys created during test
    try:
        keys_after = set(await client.keys("test:*") or [])
        keys_to_delete = keys_after - keys_before
        if keys_to_delete:
            await client.delete(*keys_to_delete)
    except Exception as e:
        logger.warning(f"Failed to cleanup test keys: {e}")

    # Close connection
    try:
        if hasattr(client, 'close'):
            await client.close()
        elif hasattr(client, 'aclose'):
            await client.aclose()
    except Exception:
        pass


@pytest.fixture
def redis_cluster_client(redis_cluster_nodes, redis_available):
    """Provide a Redis cluster client instance."""
    skip_if_no_redis(redis_available)

    try:
        from cy_redis.distributed import RedisCluster
        client = RedisCluster(nodes=redis_cluster_nodes)
    except ImportError:
        try:
            from redis.cluster import RedisCluster
            client = RedisCluster(startup_nodes=redis_cluster_nodes)
        except ImportError:
            pytest.skip("Redis cluster client not available")

    yield client

    # Cleanup
    try:
        if hasattr(client, 'close'):
            client.close()
    except Exception:
        pass


# ============================================================================
# Test Data Generators
# ============================================================================

@pytest.fixture
def random_key() -> Generator[callable, None, None]:
    """Generate random test keys with cleanup."""
    import random
    import string
    created_keys = []

    def _generate(prefix: str = "test") -> str:
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        key = f"{prefix}:{suffix}:{int(time.time())}"
        created_keys.append(key)
        return key

    yield _generate

    # Cleanup tracked keys
    # (actual deletion happens in redis_client fixture cleanup)


@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Provide sample data for testing."""
    return {
        "string": "test_value",
        "number": 42,
        "float": 3.14159,
        "list": [1, 2, 3, 4, 5],
        "dict": {"key1": "value1", "key2": "value2"},
        "nested": {
            "level1": {
                "level2": {
                    "data": "nested_value"
                }
            }
        },
        "bytes": b"binary_data",
        "unicode": "测试数据",
    }


@pytest.fixture
def bulk_test_data(sample_data) -> Generator[callable, None, None]:
    """Generate bulk test data."""
    def _generate(count: int = 100) -> List[Dict[str, Any]]:
        return [
            {**sample_data, "id": i, "timestamp": time.time()}
            for i in range(count)
        ]

    yield _generate


# ============================================================================
# Performance Measurement Utilities
# ============================================================================

class PerformanceTimer:
    """Context manager for measuring execution time."""

    def __init__(self, name: str = "operation"):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.duration = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.duration = self.end_time - self.start_time
        logger.info(f"{self.name} took {self.duration:.6f} seconds")
        return False

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.duration is not None:
            return self.duration
        elif self.start_time is not None:
            return time.perf_counter() - self.start_time
        return 0.0


@pytest.fixture
def perf_timer():
    """Provide a performance timer for benchmarking."""
    return PerformanceTimer


@pytest.fixture
def benchmark_results():
    """Collect benchmark results across tests."""
    results = {}
    yield results

    # Log final results
    if results:
        logger.info("=" * 60)
        logger.info("Benchmark Results:")
        for name, data in results.items():
            logger.info(f"  {name}: {data}")
        logger.info("=" * 60)


# ============================================================================
# Docker Service Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def docker_compose_file(project_root) -> Path:
    """Return path to docker-compose file if it exists."""
    compose_file = project_root / "docker-compose.yml"
    if not compose_file.exists():
        # Create a basic docker-compose.yml for tests
        compose_file = project_root / "tests" / "docker-compose.test.yml"
    return compose_file


@pytest.fixture(scope="session")
def docker_services(docker_available, docker_compose_file):
    """
    Start Docker services for testing.
    Requires docker-compose file to be present.
    """
    skip_if_no_docker(docker_available)

    if not docker_compose_file.exists():
        pytest.skip("Docker compose file not found")

    import subprocess

    # Start services
    try:
        subprocess.run(
            ["docker", "compose", "-f", str(docker_compose_file), "up", "-d"],
            check=True,
            capture_output=True
        )
        # Wait for services to be ready
        time.sleep(5)

        yield

        # Cleanup: stop services
        subprocess.run(
            ["docker", "compose", "-f", str(docker_compose_file), "down"],
            check=True,
            capture_output=True
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Docker compose failed: {e}")
        pytest.skip("Failed to start Docker services")


# ============================================================================
# PostgreSQL Fixtures (for pgcache tests)
# ============================================================================

@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    """PostgreSQL DSN for testing."""
    return os.getenv(
        "POSTGRES_TEST_DSN",
        "postgresql://postgres:postgres@localhost:5432/test_db"
    )


@pytest.fixture
def postgres_connection(postgres_dsn):
    """Provide PostgreSQL connection for testing."""
    try:
        import psycopg2
        conn = psycopg2.connect(postgres_dsn)
        yield conn
        conn.close()
    except ImportError:
        pytest.skip("psycopg2 not installed")
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")


# ============================================================================
# Async Event Loop Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop for async tests")
    except ImportError:
        logger.info("Using default asyncio event loop")

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ============================================================================
# Cleanup Utilities
# ============================================================================

@pytest.fixture(autouse=True)
def cleanup_temp_files():
    """Cleanup temporary files after each test."""
    temp_files = []

    def register(filepath: Path):
        """Register a temp file for cleanup."""
        temp_files.append(filepath)

    yield register

    # Cleanup
    for filepath in temp_files:
        try:
            if filepath.exists():
                filepath.unlink()
        except Exception as e:
            logger.warning(f"Failed to cleanup temp file {filepath}: {e}")


@pytest.fixture
def test_namespace(random_key) -> str:
    """Provide a unique namespace for test isolation."""
    return random_key("namespace")


# ============================================================================
# Test Configuration Helpers
# ============================================================================

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Provide test configuration."""
    return {
        "timeout": int(os.getenv("TEST_TIMEOUT", "30")),
        "max_retries": int(os.getenv("TEST_MAX_RETRIES", "3")),
        "retry_delay": float(os.getenv("TEST_RETRY_DELAY", "1.0")),
        "verbose": os.getenv("TEST_VERBOSE", "false").lower() == "true",
    }


# ============================================================================
# Pytest Hooks
# ============================================================================

def pytest_configure(config):
    """Configure pytest with custom settings."""
    config.addinivalue_line(
        "markers",
        "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as an integration test"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test location."""
    for item in items:
        # Add markers based on test file location
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)

        # Skip tests in CI if marked
        if item.get_closest_marker("skip_ci") and os.getenv("CI"):
            item.add_marker(pytest.mark.skip(reason="Skipped in CI environment"))


def pytest_report_header(config):
    """Add custom header to pytest report."""
    return [
        "CyRedis Test Suite",
        f"Project Root: {Path(__file__).parent.parent}",
        f"Python Path: {sys.executable}",
    ]
