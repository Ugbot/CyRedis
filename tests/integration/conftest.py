"""
Pytest fixtures for CyRedis integration tests.
"""

import pytest
import time
import os
from typing import Generator

try:
    import redis as redis_py
    from redis import Redis
    from redis.cluster import RedisCluster
    from redis.sentinel import Sentinel
    REDIS_PY_AVAILABLE = True
except ImportError:
    redis_py = None
    Redis = None
    RedisCluster = None
    Sentinel = None
    REDIS_PY_AVAILABLE = False

# Import CyRedis components
try:
    from cy_redis import CyRedisClient, CyDistributedLock as DistributedLock
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
REDIS_CLUSTER_NODES = os.getenv("REDIS_CLUSTER_NODES", "localhost:7000,localhost:7001,localhost:7002")

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
def redis_cluster_client(redis_available):
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
def redis_sentinel_client(redis_available):
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
    config.addinivalue_line(
        "markers", "game_module: mark test as requiring cy_game Redis module on port 6380"
    )
