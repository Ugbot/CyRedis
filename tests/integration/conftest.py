"""
Pytest fixtures for CyRedis integration tests.
"""

import pytest
import time
import os
from typing import Generator

try:
    from redis import Redis
    from redis.cluster import RedisCluster
    from redis.sentinel import Sentinel
    REDIS_PY_AVAILABLE = True
except ImportError:
    REDIS_PY_AVAILABLE = False

# Import CyRedis components
try:
    from cy_redis import CyRedisClient
    from redis_wrapper import (
        HighPerformanceRedis,
        DistributedLock,
        ReliableQueue,
        WorkerQueue,
        StreamConsumerGroup,
    )
    CYREDIS_AVAILABLE = True
except ImportError:
    CYREDIS_AVAILABLE = False


# Redis connection configuration from environment
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Cluster configuration
REDIS_CLUSTER_NODES = os.getenv("REDIS_CLUSTER_NODES", "localhost:7000,localhost:7001,localhost:7002")

# Sentinel configuration
REDIS_SENTINEL_HOSTS = os.getenv("REDIS_SENTINEL_HOSTS", "localhost:26379")
REDIS_SENTINEL_MASTER = os.getenv("REDIS_SENTINEL_MASTER", "mymaster")


@pytest.fixture(scope="session")
def redis_available() -> bool:
    """Check if Redis is available."""
    if not REDIS_PY_AVAILABLE:
        return False

    try:
        client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                      password=REDIS_PASSWORD, socket_timeout=2)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


@pytest.fixture
def redis_client(redis_available) -> Generator[Redis, None, None]:
    """Provide a standard Redis client for testing."""
    if not redis_available:
        pytest.skip("Redis not available")

    client = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True
    )

    yield client

    # Cleanup - flush test keys
    try:
        test_keys = client.keys("test:*")
        if test_keys:
            client.delete(*test_keys)
    except Exception:
        pass
    finally:
        client.close()


@pytest.fixture
def cyredis_client(redis_available) -> Generator[CyRedisClient, None, None]:
    """Provide a CyRedis client for testing."""
    if not redis_available:
        pytest.skip("Redis not available")
    if not CYREDIS_AVAILABLE:
        pytest.skip("CyRedis not built")

    client = CyRedisClient(
        host=REDIS_HOST.encode(),
        port=REDIS_PORT,
        db=REDIS_DB
    )

    yield client

    # Cleanup
    try:
        # CyRedis uses bytes
        client.delete(b"test:*")
    except Exception:
        pass
    finally:
        del client


@pytest.fixture
def hp_redis_client(redis_available) -> Generator[HighPerformanceRedis, None, None]:
    """Provide a HighPerformanceRedis client for testing."""
    if not redis_available:
        pytest.skip("Redis not available")
    if not CYREDIS_AVAILABLE:
        pytest.skip("CyRedis not built")

    client = HighPerformanceRedis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD
    )

    yield client

    # Cleanup
    try:
        test_keys = client.keys("test:*")
        if test_keys:
            client.delete(*test_keys)
    except Exception:
        pass
    finally:
        client.close()


@pytest.fixture
def redis_cluster_client(redis_available) -> Generator[RedisCluster, None, None]:
    """Provide a Redis Cluster client for testing."""
    if not redis_available:
        pytest.skip("Redis not available")
    if not REDIS_PY_AVAILABLE:
        pytest.skip("redis-py not available")

    # Parse cluster nodes
    nodes = []
    for node_str in REDIS_CLUSTER_NODES.split(","):
        host, port = node_str.strip().split(":")
        nodes.append({"host": host, "port": int(port)})

    try:
        client = RedisCluster(
            startup_nodes=nodes,
            decode_responses=True,
            skip_full_coverage_check=True
        )
        client.ping()
    except Exception:
        pytest.skip("Redis Cluster not available")

    yield client

    # Cleanup
    try:
        test_keys = client.keys("test:*")
        if test_keys:
            client.delete(*test_keys)
    except Exception:
        pass
    finally:
        client.close()


@pytest.fixture
def redis_sentinel_client(redis_available) -> Generator[Redis, None, None]:
    """Provide a Redis Sentinel client for testing."""
    if not redis_available:
        pytest.skip("Redis not available")
    if not REDIS_PY_AVAILABLE:
        pytest.skip("redis-py not available")

    # Parse sentinel hosts
    sentinels = []
    for host_str in REDIS_SENTINEL_HOSTS.split(","):
        host, port = host_str.strip().split(":")
        sentinels.append((host, int(port)))

    try:
        sentinel = Sentinel(sentinels, socket_timeout=2)
        client = sentinel.master_for(
            REDIS_SENTINEL_MASTER,
            decode_responses=True
        )
        client.ping()
    except Exception:
        pytest.skip("Redis Sentinel not available")

    yield client

    # Cleanup
    try:
        test_keys = client.keys("test:*")
        if test_keys:
            client.delete(*test_keys)
    except Exception:
        pass


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
def distributed_lock(hp_redis_client, unique_key) -> DistributedLock:
    """Provide a distributed lock for testing."""
    lock_key = f"{unique_key}:lock"
    return DistributedLock(hp_redis_client, lock_key)


@pytest.fixture
def reliable_queue(hp_redis_client, unique_key) -> ReliableQueue:
    """Provide a reliable queue for testing."""
    queue_name = f"{unique_key}:queue"
    return ReliableQueue(hp_redis_client, queue_name)


@pytest.fixture
def worker_queue(hp_redis_client, unique_key) -> WorkerQueue:
    """Provide a worker queue for testing."""
    queue_name = f"{unique_key}:worker"
    return WorkerQueue(hp_redis_client, queue_name)


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "cluster: mark test as requiring Redis Cluster"
    )
    config.addinivalue_line(
        "markers", "sentinel: mark test as requiring Redis Sentinel"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance benchmark"
    )
