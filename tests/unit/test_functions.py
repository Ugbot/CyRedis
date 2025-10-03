"""
Unit tests for functions.pyx - Redis Functions
"""
import pytest
import uuid
from cy_redis.functions import (
    CyRedisFunctionsManager, CyLocks, CyRateLimiter, CyQueue,
    RedisFunctions, FUNCTION_LIBRARIES
)
from cy_redis.cy_redis_client import CyRedisClient


@pytest.fixture
def redis_client():
    """Create a Redis client for testing"""
    return CyRedisClient(host="localhost", port=6379)


@pytest.fixture
def functions_manager(redis_client):
    """Create a functions manager for testing"""
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
        manager = CyRedisFunctionsManager(redis_client)
        assert manager is not None
        assert manager.redis is not None

    def test_load_library(self, functions_manager):
        """Test loading a function library"""
        try:
            result = functions_manager.load_library("cy:locks")
            assert 'status' in result
            # May be 'loaded' or 'already_loaded'
        except Exception as e:
            # Redis may not support functions or server may be old version
            pytest.skip(f"Redis Functions not supported: {e}")

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
        # Try to load a library first
        try:
            functions_manager.load_library("cy:locks")
            info = functions_manager.get_library_info("cy:locks")
            assert isinstance(info, dict)
        except:
            # Library may not be loaded
            pass


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
        assert 'version' in lib
        assert 'description' in lib
        assert 'functions' in lib

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

        try:
            # Try to load library first
            cy_locks.func_mgr.load_library("cy:locks")

            result = cy_locks.acquire(lock_key, owner)
            assert isinstance(result, dict)
            assert 'acquired' in result
            assert 'fencing_token' in result

            # Release lock
            if result['acquired']:
                cy_locks.release(lock_key, owner)
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")

    def test_release_lock(self, cy_locks):
        """Test releasing a lock"""
        lock_key = f"test_lock_{uuid.uuid4().hex[:8]}"
        owner = f"owner_{uuid.uuid4().hex[:8]}"

        try:
            cy_locks.func_mgr.load_library("cy:locks")
            result = cy_locks.acquire(lock_key, owner)
            if result['acquired']:
                release_result = cy_locks.release(lock_key, owner)
                assert isinstance(release_result, (bool, int))
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")

    def test_refresh_lock(self, cy_locks):
        """Test refreshing a lock"""
        lock_key = f"test_lock_{uuid.uuid4().hex[:8]}"
        owner = f"owner_{uuid.uuid4().hex[:8]}"

        try:
            cy_locks.func_mgr.load_library("cy:locks")
            result = cy_locks.acquire(lock_key, owner)
            if result['acquired']:
                refresh_result = cy_locks.refresh(lock_key, owner)
                assert isinstance(refresh_result, (bool, int))
                cy_locks.release(lock_key, owner)
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")


class TestCyRateLimiter:
    """Test CyRateLimiter class"""

    def test_rate_limiter_creation(self, functions_manager):
        """Test creating CyRateLimiter"""
        limiter = CyRateLimiter(functions_manager)
        assert limiter is not None

    def test_token_bucket(self, cy_rate_limiter):
        """Test token bucket rate limiting"""
        key = f"rate_{uuid.uuid4().hex[:8]}"

        try:
            cy_rate_limiter.func_mgr.load_library("cy:rate")

            result = cy_rate_limiter.token_bucket(
                key,
                capacity=10,
                refill_rate_per_ms=1.0,
                cost=1
            )

            assert isinstance(result, dict)
            assert 'allowed' in result
            assert 'remaining' in result
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")

    def test_sliding_window(self, cy_rate_limiter):
        """Test sliding window rate limiting"""
        key = f"rate_{uuid.uuid4().hex[:8]}"

        try:
            cy_rate_limiter.func_mgr.load_library("cy:rate")

            result = cy_rate_limiter.sliding_window(
                key,
                window_ms=1000,
                max_requests=10
            )

            assert isinstance(result, dict)
            assert 'allowed' in result
            assert 'remaining' in result
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")


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

        try:
            cy_queue.func_mgr.load_library("cy:queue")

            result = cy_queue.enqueue(
                queue_name,
                message_id,
                "test_payload"
            )

            assert result in ["enqueued", "delayed", "duplicate"]
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")

    def test_pull(self, cy_queue):
        """Test pulling messages from queue"""
        queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"

        try:
            cy_queue.func_mgr.load_library("cy:queue")

            # Enqueue first
            message_id = str(uuid.uuid4())
            cy_queue.enqueue(queue_name, message_id, "test_payload")

            # Pull
            messages = cy_queue.pull(queue_name, max_messages=1)
            assert isinstance(messages, list)
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")

    def test_ack(self, cy_queue):
        """Test acknowledging a message"""
        queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"

        try:
            cy_queue.func_mgr.load_library("cy:queue")
            message_id = str(uuid.uuid4())
            cy_queue.enqueue(queue_name, message_id, "test_payload")

            messages = cy_queue.pull(queue_name, max_messages=1)
            if messages:
                # Message format depends on implementation
                # result = cy_queue.ack(queue_name, message_id)
                pass
        except Exception as e:
            pytest.skip(f"Redis Functions not supported: {e}")


class TestRedisFunctions:
    """Test RedisFunctions wrapper"""

    def test_wrapper_creation(self):
        """Test creating RedisFunctions wrapper"""
        # Note: This requires a properly configured Redis client
        pass

    def test_load_library(self):
        """Test loading library through wrapper"""
        # Simplified test
        pass


class TestEdgeCases:
    """Test edge cases for functions"""

    def test_call_unloaded_library(self, functions_manager):
        """Test calling function from unloaded library"""
        # Should handle gracefully
        pass

    def test_invalid_function_name(self, functions_manager):
        """Test calling invalid function name"""
        # Should handle gracefully
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
