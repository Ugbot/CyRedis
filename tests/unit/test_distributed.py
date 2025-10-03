"""
Unit tests for distributed.pyx - Distributed locks and primitives
"""
import pytest
import time
import threading
import uuid
from cy_redis.distributed import CyDistributedLock, CyReadWriteLock
from cy_redis.cy_redis_client import CyRedisClient


@pytest.fixture
def redis_client():
    """Create a Redis client for testing"""
    return CyRedisClient(host="localhost", port=6379)


@pytest.fixture
def distributed_lock(redis_client):
    """Create a distributed lock for testing"""
    lock_key = f"test_lock_{uuid.uuid4().hex[:8]}"
    lock = CyDistributedLock(
        redis_client,
        lock_key,
        ttl_ms=30000
    )
    yield lock
    # Cleanup
    try:
        lock.release()
        redis_client.delete(lock_key)
    except:
        pass


@pytest.fixture
def read_write_lock(redis_client):
    """Create a read-write lock for testing"""
    lock_key = f"test_rwlock_{uuid.uuid4().hex[:8]}"
    rwlock = CyReadWriteLock(redis_client, lock_key, ttl_ms=30000)
    yield rwlock
    # Cleanup
    try:
        rwlock.release_read_lock()
        rwlock.release_write_lock()
        redis_client.delete(f"{lock_key}:read")
        redis_client.delete(f"{lock_key}:write")
    except:
        pass


class TestCyDistributedLock:
    """Test CyDistributedLock class"""

    def test_lock_creation(self, redis_client):
        """Test creating a distributed lock"""
        lock = CyDistributedLock(redis_client, "test_lock", ttl_ms=30000)
        assert lock is not None
        assert lock.lock_key == "test_lock"
        assert lock.ttl_ms == 30000

    def test_acquire_lock(self, distributed_lock):
        """Test acquiring a lock"""
        result = distributed_lock.try_acquire(blocking=False)
        assert result is True

        # Lock should be locked
        assert distributed_lock.is_locked() is True

        # Release lock
        distributed_lock.release()

    def test_acquire_blocking(self, distributed_lock):
        """Test blocking lock acquisition"""
        result = distributed_lock.try_acquire(blocking=True, timeout=1.0)
        assert result is True
        distributed_lock.release()

    def test_acquire_already_locked(self, distributed_lock):
        """Test acquiring an already locked lock"""
        # First acquisition
        result1 = distributed_lock.try_acquire(blocking=False)
        assert result1 is True

        # Second acquisition should fail
        result2 = distributed_lock.try_acquire(blocking=False)
        assert result2 is False

        distributed_lock.release()

    def test_release_lock(self, distributed_lock):
        """Test releasing a lock"""
        distributed_lock.try_acquire()
        distributed_lock.release()

        # Lock should be released
        assert distributed_lock.is_locked() is False

    def test_is_locked(self, distributed_lock):
        """Test checking if lock is held"""
        assert distributed_lock.is_locked() is False

        distributed_lock.try_acquire()
        assert distributed_lock.is_locked() is True

        distributed_lock.release()
        assert distributed_lock.is_locked() is False

    def test_get_ttl(self, distributed_lock):
        """Test getting lock TTL"""
        distributed_lock.try_acquire()
        ttl = distributed_lock.get_ttl()
        assert ttl > 0
        assert ttl <= 30000

        distributed_lock.release()

    def test_extend_lock(self, distributed_lock):
        """Test extending lock TTL"""
        distributed_lock.try_acquire()

        initial_ttl = distributed_lock.get_ttl()
        result = distributed_lock.extend(ttl_ms=60000)
        assert result is True

        new_ttl = distributed_lock.get_ttl()
        assert new_ttl > initial_ttl

        distributed_lock.release()

    def test_context_manager(self, distributed_lock):
        """Test using lock as context manager"""
        with distributed_lock:
            assert distributed_lock.is_locked() is True

        # Lock should be released after context
        assert distributed_lock.is_locked() is False

    def test_concurrent_access(self, redis_client):
        """Test concurrent lock access from multiple threads"""
        lock_key = f"concurrent_lock_{uuid.uuid4().hex[:8]}"
        acquired_count = []

        def worker():
            lock = CyDistributedLock(redis_client, lock_key, ttl_ms=5000)
            if lock.try_acquire(blocking=True, timeout=2.0):
                acquired_count.append(1)
                time.sleep(0.1)
                lock.release()

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should have acquired the lock sequentially
        assert len(acquired_count) == 5

        # Cleanup
        redis_client.delete(lock_key)


class TestCyReadWriteLock:
    """Test CyReadWriteLock class"""

    def test_rwlock_creation(self, redis_client):
        """Test creating a read-write lock"""
        rwlock = CyReadWriteLock(redis_client, "test_rwlock", ttl_ms=30000)
        assert rwlock is not None
        assert rwlock.base_key == "test_rwlock"

    def test_acquire_read_lock(self, read_write_lock):
        """Test acquiring a read lock"""
        result = read_write_lock.try_read_lock(blocking=False)
        assert result is True

        read_write_lock.release_read_lock()

    def test_multiple_readers(self, read_write_lock):
        """Test multiple concurrent readers"""
        result1 = read_write_lock.try_read_lock(blocking=False)
        result2 = read_write_lock.try_read_lock(blocking=False)

        assert result1 is True
        assert result2 is True

        read_write_lock.release_read_lock()
        read_write_lock.release_read_lock()

    def test_acquire_write_lock(self, read_write_lock):
        """Test acquiring a write lock"""
        result = read_write_lock.try_write_lock(blocking=False)
        assert result is True

        read_write_lock.release_write_lock()

    def test_write_blocks_read(self, read_write_lock):
        """Test that write lock blocks readers"""
        # Acquire write lock
        write_result = read_write_lock.try_write_lock(blocking=False)
        assert write_result is True

        # Try to acquire read lock (should fail)
        read_result = read_write_lock.try_read_lock(blocking=False)
        assert read_result is False

        read_write_lock.release_write_lock()

    def test_read_blocks_write(self, read_write_lock):
        """Test that read lock blocks writers"""
        # Acquire read lock
        read_result = read_write_lock.try_read_lock(blocking=False)
        assert read_result is True

        # Try to acquire write lock (should fail)
        write_result = read_write_lock.try_write_lock(blocking=False)
        assert write_result is False

        read_write_lock.release_read_lock()

    def test_get_stats(self, read_write_lock):
        """Test getting read-write lock statistics"""
        read_write_lock.try_read_lock()
        stats = read_write_lock.get_stats()

        assert 'readers' in stats
        assert 'writer' in stats
        assert stats['readers'] > 0

        read_write_lock.release_read_lock()


class TestLockEdgeCases:
    """Test edge cases for distributed locks"""

    def test_lock_timeout(self, redis_client):
        """Test lock timeout behavior"""
        lock_key = f"timeout_lock_{uuid.uuid4().hex[:8]}"
        lock = CyDistributedLock(redis_client, lock_key, ttl_ms=1000)

        lock.try_acquire()
        # Wait for lock to expire
        time.sleep(1.5)

        # Lock should have expired
        assert lock.is_locked() is False

        # Cleanup
        redis_client.delete(lock_key)

    def test_release_unlocked_lock(self, distributed_lock):
        """Test releasing an unlocked lock"""
        # Should not raise error
        distributed_lock.release()

    def test_extend_unlocked_lock(self, distributed_lock):
        """Test extending an unlocked lock"""
        result = distributed_lock.extend()
        assert result is False

    def test_concurrent_write_locks(self, redis_client):
        """Test concurrent write lock acquisition"""
        lock_key = f"concurrent_write_{uuid.uuid4().hex[:8]}"
        acquired = []

        def writer():
            rwlock = CyReadWriteLock(redis_client, lock_key)
            if rwlock.try_write_lock(blocking=True, timeout=2.0):
                acquired.append(1)
                time.sleep(0.1)
                rwlock.release_write_lock()

        threads = [threading.Thread(target=writer) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Only one writer should have acquired at a time
        assert len(acquired) == 3

        # Cleanup
        redis_client.delete(f"{lock_key}:read")
        redis_client.delete(f"{lock_key}:write")


class TestAsyncLocks:
    """Test async lock operations"""

    @pytest.mark.asyncio
    async def test_async_acquire(self, redis_client):
        """Test async lock acquisition"""
        lock = CyDistributedLock(redis_client, "async_lock")
        result = await lock.try_acquire_async(blocking=False)
        # Clean up regardless of result
        try:
            await lock.release_async()
        except:
            pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
