"""
Pytest fixtures for CyRedis integration tests.
"""

import os
import time
from typing import Generator

import pytest

# Import CyRedis components
try:
    from cy_redis import CyDistributedLock as DistributedLock
    from cy_redis import CyRedisClient
    from cy_redis.reliable_queue import ReliableQueue, WorkerQueue

    try:
        from cy_redis.high_performance_redis import HighPerformanceRedis
    except ImportError:
        HighPerformanceRedis = None
    CYREDIS_AVAILABLE = True
except ImportError:
    CYREDIS_AVAILABLE = False


# Redis connection configuration from environment. Integration tests run
# against a dedicated logical DB (15 by default) and FLUSHDB it on teardown,
# so they never touch application data on db 0.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "15"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")


def _make_cyredis_client():
    """Build a CyRedisClient on the configured test DB."""
    return CyRedisClient(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


# Cluster configuration
REDIS_CLUSTER_NODES = os.getenv(
    "REDIS_CLUSTER_NODES", "localhost:7000,localhost:7001,localhost:7002"
)

# Sentinel configuration
REDIS_SENTINEL_HOSTS = os.getenv("REDIS_SENTINEL_HOSTS", "localhost:26379")
REDIS_SENTINEL_MASTER = os.getenv("REDIS_SENTINEL_MASTER", "mymaster")


@pytest.fixture(scope="session")
def redis_available() -> bool:
    """Check if Redis is available, using the CyRedis client itself."""
    if not CYREDIS_AVAILABLE:
        return False
    try:
        client = _make_cyredis_client()
        ok = client.execute_command(["PING"]) in ("PONG", b"PONG")
        return ok
    except Exception:
        return False


@pytest.fixture
def redis_client(redis_available):
    """Provide a CyRedis client for testing (the library under test — never
    redis-py, per the project's replacement-library contract)."""
    if not CYREDIS_AVAILABLE:
        pytest.skip("CyRedis not built")
    if not redis_available:
        pytest.skip("Redis not available")

    client = _make_cyredis_client()
    yield client

    # Cleanup: the test DB is dedicated, so flush it wholesale.
    try:
        client.execute_command(["FLUSHDB"])
    except Exception:
        pass


# cyredis_client is an explicit alias of redis_client now that the standard
# fixture already yields a CyRedis client; kept for tests that request it.
@pytest.fixture
def cyredis_client(redis_client):
    """Provide a CyRedis client for testing (alias of redis_client)."""
    return redis_client


@pytest.fixture
def hp_redis_client(redis_available):
    """Provide a CyRedis client for the worker/lock/queue tests.

    Yields a real CyRedisClient so the Cython managers and locks (which are
    typed against it) accept it directly.
    """
    if not CYREDIS_AVAILABLE:
        pytest.skip("CyRedis not built")
    if not redis_available:
        pytest.skip("Redis not available")

    client = _make_cyredis_client()
    yield client
    try:
        client.execute_command(["FLUSHDB"])
    except Exception:
        pass


@pytest.fixture
def benchmark_config():
    """Configuration for the performance/benchmark tests."""
    return {
        "iterations": int(os.getenv("BENCHMARK_ITERATIONS", "1000")),
        "concurrency": int(os.getenv("BENCHMARK_CONCURRENCY", "8")),
        "warmup": int(os.getenv("BENCHMARK_WARMUP", "100")),
        "value_size": int(os.getenv("BENCHMARK_VALUE_SIZE", "100")),
    }


@pytest.fixture
def redis_cluster_client():
    """A CyRedis cluster client, skipped when no cluster is running.

    The cluster is a separate deployment from the single server the rest of
    the suite uses, so `redis_available` says nothing about it.
    """
    from cy_redis.core.cluster import CyRedisCluster

    nodes = [node.strip() for node in REDIS_CLUSTER_NODES.split(",") if node.strip()]
    try:
        client = CyRedisCluster(nodes=nodes)
        client.ping()
    except Exception as exc:
        pytest.skip(f"No Redis Cluster at {REDIS_CLUSTER_NODES}: {exc}")

    yield client

    try:
        test_keys = client.keys("test:*")
        if test_keys:
            client.delete(*test_keys)
    finally:
        client.close()


@pytest.fixture
def redis_sentinel_client():
    """A CyRedis client for the sentinel-monitored master."""
    from cy_redis.core.sentinel import CySentinel

    sentinels = [
        host.strip() for host in REDIS_SENTINEL_HOSTS.split(",") if host.strip()
    ]
    try:
        sentinel = CySentinel(sentinels, socket_timeout=2)
        client = sentinel.master_for(REDIS_SENTINEL_MASTER)
        client.ping()
    except Exception as exc:
        pytest.skip(f"No Redis Sentinel at {REDIS_SENTINEL_HOSTS}: {exc}")

    yield client

    try:
        test_keys = client.keys("test:*")
        if test_keys:
            client.delete(*test_keys)
    finally:
        client.close()
        sentinel.close()


@pytest.fixture
def unique_key() -> str:
    """Generate a unique test key."""
    return f"test:{int(time.time() * 1000000)}"


@pytest.fixture
def cleanup_keys(redis_client):
    """Fixture to track and cleanup keys after test."""
    keys_to_cleanup = []

    def add_key(key: str):
        keys_to_cleanup.append(key)
        return key

    yield add_key

    # Cleanup
    if keys_to_cleanup:
        try:
            redis_client.delete(*keys_to_cleanup)
        except Exception:
            pass


@pytest.fixture
def distributed_lock(hp_redis_client, unique_key):
    """Provide a distributed lock for testing."""
    lock_key = f"{unique_key}:lock"
    return DistributedLock(hp_redis_client, lock_key)


@pytest.fixture
def reliable_queue(hp_redis_client, unique_key):
    """Provide a reliable queue for testing."""
    queue_name = f"{unique_key}:queue"
    return ReliableQueue(hp_redis_client, queue_name)


@pytest.fixture
def worker_queue(hp_redis_client, unique_key):
    """Provide a worker queue for testing."""
    queue_name = f"{unique_key}:worker"
    return WorkerQueue(hp_redis_client, queue_name)


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "cluster: mark test as requiring Redis Cluster")
    config.addinivalue_line(
        "markers", "sentinel: mark test as requiring Redis Sentinel"
    )
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line(
        "markers", "performance: mark test as performance benchmark"
    )
    config.addinivalue_line(
        "markers",
        "game_module: mark test as requiring cy_game Redis module on port 6380",
    )
