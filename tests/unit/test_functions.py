"""
Unit tests for functions.pyx - Redis Functions
"""

import json
import uuid

import pytest

from cy_redis.core.cy_redis_client import CyRedisClient
from cy_redis.features.functions import (
    FUNCTION_LIBRARIES,
    CyLocks,
    CyQueue,
    CyRateLimiter,
    CyRedisFunctionsManager,
    RedisFunctions,
)
from tests.server_env import REDIS_HOST, REDIS_PORT


@pytest.fixture
def redis_client():
    """Create a Redis client for testing"""
    return CyRedisClient(host=REDIS_HOST, port=REDIS_PORT)


@pytest.fixture(scope="session")
def server_supports_functions():
    """Whether the server under test implements FUNCTION (Redis 7+, Valkey 7+)."""
    client = CyRedisClient(host=REDIS_HOST, port=REDIS_PORT)
    try:
        client.execute_command(["FUNCTION", "LIST"])
    except Exception:
        return False
    return True


@pytest.fixture
def functions_manager(redis_client, server_supports_functions):
    """Create a functions manager for testing"""
    if not server_supports_functions:
        pytest.skip("server has no FUNCTION support")
    manager = CyRedisFunctionsManager(redis_client)
    yield manager


@pytest.fixture
def cy_locks(functions_manager):
    """Create CyLocks instance"""
    return CyLocks(functions_manager)


@pytest.fixture
def cy_rate_limiter(functions_manager):
    """Create CyRateLimiter instance"""
    return CyRateLimiter(functions_manager)


@pytest.fixture
def cy_queue(functions_manager):
    """Create CyQueue instance"""
    return CyQueue(functions_manager)


class TestCyRedisFunctionsManager:
    """Test CyRedisFunctionsManager class"""

    def test_manager_creation(self, redis_client):
        """Test creating functions manager"""
        manager = CyRedisFunctionsManager(redis_client)  # no server round trip
        assert manager is not None
        assert manager.redis is not None

    def test_load_library(self, functions_manager):
        """Test loading a function library"""
        result = functions_manager.load_library("cy:locks")
        assert result["status"] in ("loaded", "already_loaded")
        assert result["functions"] == FUNCTION_LIBRARIES["cy:locks"]["functions"]

    def test_load_invalid_library(self, functions_manager):
        """Test loading invalid library"""
        with pytest.raises(ValueError):
            functions_manager.load_library("invalid_library")

    def test_list_loaded_libraries(self, functions_manager):
        """Test listing loaded libraries"""
        libraries = functions_manager.list_loaded_libraries()
        assert isinstance(libraries, list)

    def test_get_library_info(self, functions_manager):
        """Test getting library information"""
        functions_manager.load_library("cy:locks")
        info = functions_manager.get_library_info("cy:locks")
        assert info["name"] == "cy:locks"
        assert info["functions"] == FUNCTION_LIBRARIES["cy:locks"]["functions"]


class TestFunctionLibraries:
    """Test function library definitions"""

    def test_function_libraries_defined(self):
        """Test that function libraries are defined"""
        assert isinstance(FUNCTION_LIBRARIES, dict)
        assert len(FUNCTION_LIBRARIES) > 0

    def test_locks_library_defined(self):
        """Test locks library definition"""
        assert "cy:locks" in FUNCTION_LIBRARIES
        lib = FUNCTION_LIBRARIES["cy:locks"]
        assert "version" in lib
        assert "description" in lib
        assert "functions" in lib

    def test_rate_library_defined(self):
        """Test rate limiting library definition"""
        assert "cy:rate" in FUNCTION_LIBRARIES

    def test_queue_library_defined(self):
        """Test queue library definition"""
        assert "cy:queue" in FUNCTION_LIBRARIES


class TestCyLocks:
    """Test CyLocks class"""

    def test_locks_creation(self, functions_manager):
        """Test creating CyLocks"""
        locks = CyLocks(functions_manager)
        assert locks is not None

    def test_acquire_lock(self, cy_locks):
        """Test acquiring a lock"""
        lock_key = f"test_lock_{uuid.uuid4().hex[:8]}"
        owner = f"owner_{uuid.uuid4().hex[:8]}"

        cy_locks.func_mgr.load_library("cy:locks")

        result = cy_locks.acquire(lock_key, owner)
        assert result["acquired"] is True
        assert result["fencing_token"] >= 1

        # A second owner must not get the same lock.
        contended = cy_locks.acquire(lock_key, f"other_{owner}")
        assert contended["acquired"] is False

        cy_locks.release(lock_key, owner)

    def test_release_lock(self, cy_locks):
        """Test releasing a lock"""
        lock_key = f"test_lock_{uuid.uuid4().hex[:8]}"
        owner = f"owner_{uuid.uuid4().hex[:8]}"

        cy_locks.func_mgr.load_library("cy:locks")
        assert cy_locks.acquire(lock_key, owner)["acquired"] is True
        assert cy_locks.release(lock_key, owner) is True
        # Once released the lock is free again.
        assert cy_locks.acquire(lock_key, f"other_{owner}")["acquired"] is True

    def test_refresh_lock(self, cy_locks):
        """Test refreshing a lock"""
        lock_key = f"test_lock_{uuid.uuid4().hex[:8]}"
        owner = f"owner_{uuid.uuid4().hex[:8]}"

        cy_locks.func_mgr.load_library("cy:locks")
        assert cy_locks.acquire(lock_key, owner)["acquired"] is True
        assert cy_locks.refresh(lock_key, owner) is True
        # A non-owner cannot refresh someone else's lock.
        assert cy_locks.refresh(lock_key, f"other_{owner}") is False
        cy_locks.release(lock_key, owner)


class TestCyRateLimiter:
    """Test CyRateLimiter class"""

    def test_rate_limiter_creation(self, functions_manager):
        """Test creating CyRateLimiter"""
        limiter = CyRateLimiter(functions_manager)
        assert limiter is not None

    def test_token_bucket(self, cy_rate_limiter):
        """Test token bucket rate limiting"""
        key = f"rate_{uuid.uuid4().hex[:8]}"

        cy_rate_limiter.func_mgr.load_library("cy:rate")

        result = cy_rate_limiter.token_bucket(
            key, capacity=2, refill_interval_ms=60_000, cost=1
        )
        assert result["allowed"] is True
        assert result["remaining"] == 1

        # The bucket drains and then refuses, since refill is a minute away.
        assert cy_rate_limiter.token_bucket(
            key, capacity=2, refill_interval_ms=60_000, cost=1
        )["allowed"] is True
        exhausted = cy_rate_limiter.token_bucket(
            key, capacity=2, refill_interval_ms=60_000, cost=1
        )
        assert exhausted["allowed"] is False
        assert exhausted["retry_after_ms"] > 0

    def test_sliding_window(self, cy_rate_limiter):
        """Test sliding window rate limiting"""
        key = f"rate_{uuid.uuid4().hex[:8]}"

        cy_rate_limiter.func_mgr.load_library("cy:rate")

        result = cy_rate_limiter.sliding_window(key, window_ms=60_000, max_requests=2)
        assert result["allowed"] is True
        assert result["remaining"] == 1

        assert cy_rate_limiter.sliding_window(
            key, window_ms=60_000, max_requests=2
        )["allowed"] is True
        assert cy_rate_limiter.sliding_window(
            key, window_ms=60_000, max_requests=2
        )["allowed"] is False

    def test_leaky_bucket(self, cy_rate_limiter):
        """Test leaky bucket rate limiting"""
        key = f"rate_{uuid.uuid4().hex[:8]}"

        cy_rate_limiter.func_mgr.load_library("cy:rate")

        result = cy_rate_limiter.leaky_bucket(key, rate_per_ms=0.000001, burst=1)
        assert result["allowed"] is True
        assert cy_rate_limiter.leaky_bucket(
            key, rate_per_ms=0.000001, burst=1
        )["allowed"] is False


class TestCyQueue:
    """Test CyQueue class"""

    def test_queue_creation(self, functions_manager):
        """Test creating CyQueue"""
        queue = CyQueue(functions_manager)
        assert queue is not None

    def test_enqueue(self, cy_queue):
        """Test enqueuing a message"""
        queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"
        message_id = str(uuid.uuid4())

        cy_queue.func_mgr.load_library("cy:queue")

        assert cy_queue.enqueue(queue_name, message_id, "test_payload") == "enqueued"
        # The same id is deduplicated.
        assert cy_queue.enqueue(queue_name, message_id, "test_payload") == "duplicate"
        assert (
            cy_queue.enqueue(queue_name, str(uuid.uuid4()), "later", delay_s=60)
            == "delayed"
        )

    def test_pull(self, cy_queue):
        """Test pulling messages from queue"""
        queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"

        cy_queue.func_mgr.load_library("cy:queue")

        message_id = str(uuid.uuid4())
        cy_queue.enqueue(queue_name, message_id, "test_payload")

        messages = cy_queue.pull(queue_name, max_messages=1)
        assert len(messages) == 1
        assert json.loads(messages[0])["id"] == message_id
        # Pulled messages are invisible until their visibility timeout expires.
        assert cy_queue.pull(queue_name, max_messages=1) == []

    def test_ack(self, cy_queue):
        """Test acknowledging a message"""
        queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"

        cy_queue.func_mgr.load_library("cy:queue")
        message_id = str(uuid.uuid4())
        cy_queue.enqueue(queue_name, message_id, "test_payload")
        cy_queue.pull(queue_name, max_messages=1)

        assert cy_queue.ack(queue_name, message_id) is True
        # Acking twice reports the message is gone.
        assert cy_queue.ack(queue_name, message_id) is False

    def test_nack_requeues(self, cy_queue):
        """Test negative acknowledgement puts the message back"""
        queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"

        cy_queue.func_mgr.load_library("cy:queue")
        message_id = str(uuid.uuid4())
        cy_queue.enqueue(queue_name, message_id, "test_payload")
        cy_queue.pull(queue_name, max_messages=1)

        assert cy_queue.nack(queue_name, message_id, requeue=True) == "requeued"
        redelivered = cy_queue.pull(queue_name, max_messages=1)
        assert len(redelivered) == 1
        assert json.loads(redelivered[0])["id"] == message_id


class TestRedisFunctions:
    """Test RedisFunctions wrapper"""

    def test_wrapper_creation(self, redis_client):
        """Test creating RedisFunctions wrapper"""
        functions = RedisFunctions(redis_client)
        assert isinstance(functions.locks, CyLocks)

    def test_wrapper_rejects_foreign_client(self):
        """The wrapper only drives CyRedis clients, never another library's"""
        with pytest.raises(TypeError):
            RedisFunctions(object())


class TestEdgeCases:
    """Test edge cases for functions"""

    def test_call_unknown_function_name(self, functions_manager):
        """Calling a function no library registered surfaces the server error"""
        with pytest.raises(Exception, match="(?i)function not found"):
            functions_manager.call_function(
                f"cy_missing_{uuid.uuid4().hex[:8]}", keys=["k"], args=[]
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
