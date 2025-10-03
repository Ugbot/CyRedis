"""
Integration tests for distributed locking scenarios.

Tests distributed lock patterns including:
- Basic lock acquire/release
- Lock timeouts and TTL
- Lock contention scenarios
- Redlock algorithm
- Read/write locks
- Semaphores
- Lock renewal
"""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


try:
    from distributed import DistributedLock, ReadWriteLock, Semaphore
    LOCKS_AVAILABLE = True
except ImportError:
    LOCKS_AVAILABLE = False


@pytest.mark.integration
@pytest.mark.skipif(not LOCKS_AVAILABLE, reason="Lock components not available")
class TestBasicDistributedLock:
    """Test basic distributed lock operations."""

    def test_acquire_release(self, hp_redis_client, unique_key):
        """Test basic lock acquire and release."""
        lock_key = f"{unique_key}:lock"
        lock = DistributedLock(hp_redis_client, lock_key)

        # Acquire lock
        with lock.acquire():
            # Lock should be held
            assert hp_redis_client.exists(lock_key) == 1

        # Lock should be released
        assert hp_redis_client.exists(lock_key) == 0

    def test_lock_exclusivity(self, hp_redis_client, unique_key):
        """Test that lock provides exclusivity."""
        lock_key = f"{unique_key}:lock"
        lock1 = DistributedLock(hp_redis_client, lock_key, ttl_ms=5000)
        lock2 = DistributedLock(hp_redis_client, lock_key, ttl_ms=5000)

        # First lock acquires
        acquired1 = lock1.try_acquire(blocking=False)
        assert acquired1 is True

        # Second lock cannot acquire
        acquired2 = lock2.try_acquire(blocking=False)
        assert acquired2 is False

        # Release first lock
        lock1.release()

        # Now second lock can acquire
        acquired2 = lock2.try_acquire(blocking=False)
        assert acquired2 is True

        lock2.release()

    def test_lock_timeout(self, hp_redis_client, unique_key):
        """Test lock automatic timeout."""
        lock_key = f"{unique_key}:lock"
        lock = DistributedLock(hp_redis_client, lock_key, ttl_ms=1000)

        # Acquire lock
        acquired = lock.try_acquire(blocking=False)
        assert acquired is True

        # Wait for timeout
        time.sleep(1.5)

        # Lock should have expired
        assert hp_redis_client.exists(lock_key) == 0

    def test_lock_extension(self, hp_redis_client, unique_key):
        """Test lock TTL extension."""
        lock_key = f"{unique_key}:lock"
        lock = DistributedLock(hp_redis_client, lock_key, ttl_ms=2000)

        # Acquire lock
        acquired = lock.try_acquire(blocking=False)
        assert acquired is True

        # Wait and extend
        time.sleep(1)
        extended = lock.extend(ttl_ms=3000)
        assert extended is True

        # Lock should still exist after original TTL
        time.sleep(1.5)
        assert hp_redis_client.exists(lock_key) == 1

        # Cleanup
        lock.release()

    def test_blocking_acquire(self, hp_redis_client, unique_key):
        """Test blocking lock acquisition."""
        lock_key = f"{unique_key}:lock"
        lock1 = DistributedLock(hp_redis_client, lock_key, ttl_ms=1000)
        lock2 = DistributedLock(hp_redis_client, lock_key, ttl_ms=1000)

        # First lock acquires
        lock1.try_acquire(blocking=False)

        acquired_time = None

        def acquire_with_wait():
            nonlocal acquired_time
            # This should block until lock1 is released
            acquired = lock2.try_acquire(blocking=True, timeout=3)
            if acquired:
                acquired_time = time.time()
                lock2.release()

        # Start thread that will block
        start_time = time.time()
        thread = threading.Thread(target=acquire_with_wait)
        thread.start()

        # Wait a bit, then release
        time.sleep(0.5)
        lock1.release()

        thread.join(timeout=5)

        # Second lock should have acquired after first was released
        assert acquired_time is not None
        assert acquired_time - start_time >= 0.5


@pytest.mark.integration
@pytest.mark.skipif(not LOCKS_AVAILABLE, reason="Lock components not available")
class TestLockContention:
    """Test lock behavior under contention."""

    def test_high_contention(self, hp_redis_client, unique_key):
        """Test lock under high contention."""
        lock_key = f"{unique_key}:contended"
        counter_key = f"{unique_key}:counter"

        hp_redis_client.set(counter_key, "0")

        successful_acquires = []
        lock_obj = threading.Lock()

        def contending_worker(worker_id):
            """Worker that tries to acquire lock."""
            lock = DistributedLock(hp_redis_client, lock_key, ttl_ms=1000)

            acquired = lock.try_acquire(blocking=True, timeout=5)
            if acquired:
                try:
                    # Critical section
                    current = int(hp_redis_client.get(counter_key))
                    time.sleep(0.01)  # Simulate work
                    hp_redis_client.set(counter_key, str(current + 1))

                    with lock_obj:
                        successful_acquires.append(worker_id)
                finally:
                    lock.release()

        # Run 20 workers competing for lock
        threads = []
        for i in range(20):
            t = threading.Thread(target=contending_worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=30)

        # All workers should have acquired lock exactly once
        assert len(successful_acquires) == 20

        # Counter should be exactly 20 (no race conditions)
        final_count = int(hp_redis_client.get(counter_key))
        assert final_count == 20

        # Cleanup
        hp_redis_client.delete(counter_key)

    def test_fairness(self, hp_redis_client, unique_key):
        """Test lock fairness (no starvation)."""
        lock_key = f"{unique_key}:fair"
        acquire_order = []
        lock_obj = threading.Lock()

        def worker(worker_id):
            lock = DistributedLock(hp_redis_client, lock_key, ttl_ms=500)

            # Try to acquire 3 times
            for _ in range(3):
                acquired = lock.try_acquire(blocking=True, timeout=10)
                if acquired:
                    with lock_obj:
                        acquire_order.append(worker_id)
                    time.sleep(0.1)
                    lock.release()

        # Run workers
        threads = []
        for i in range(5):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=20)

        # Should have 15 total acquisitions (5 workers * 3 each)
        assert len(acquire_order) == 15

        # Each worker should have acquired at least once
        worker_counts = {}
        for worker_id in acquire_order:
            worker_counts[worker_id] = worker_counts.get(worker_id, 0) + 1

        assert len(worker_counts) == 5
        assert all(count > 0 for count in worker_counts.values())


@pytest.mark.integration
@pytest.mark.skipif(not LOCKS_AVAILABLE, reason="Lock components not available")
class TestReadWriteLock:
    """Test read/write lock patterns."""

    def test_multiple_readers(self, hp_redis_client, unique_key):
        """Test that multiple readers can acquire lock simultaneously."""
        lock_key = f"{unique_key}:rwlock"
        rwlock = ReadWriteLock(hp_redis_client, lock_key)

        readers_active = []
        lock_obj = threading.Lock()

        def reader(reader_id):
            with rwlock.read_lock():
                with lock_obj:
                    readers_active.append(reader_id)

                # Simulate reading
                time.sleep(0.5)

                with lock_obj:
                    readers_active.remove(reader_id)

        # Start 5 readers
        threads = []
        for i in range(5):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
            t.start()

        # Check that multiple readers are active simultaneously
        time.sleep(0.2)
        with lock_obj:
            assert len(readers_active) > 1

        for t in threads:
            t.join()

    def test_writer_exclusivity(self, hp_redis_client, unique_key):
        """Test that writer has exclusive access."""
        lock_key = f"{unique_key}:rwlock"
        rwlock = ReadWriteLock(hp_redis_client, lock_key)

        value_key = f"{unique_key}:value"
        hp_redis_client.set(value_key, "0")

        operations = []
        lock_obj = threading.Lock()

        def reader(reader_id):
            time.sleep(0.1)  # Let writer start first
            with rwlock.read_lock():
                value = hp_redis_client.get(value_key)
                with lock_obj:
                    operations.append(f"R{reader_id}:{value}")

        def writer(writer_id):
            with rwlock.write_lock():
                current = int(hp_redis_client.get(value_key))
                time.sleep(0.3)  # Simulate write work
                hp_redis_client.set(value_key, str(current + 1))
                with lock_obj:
                    operations.append(f"W{writer_id}:{current + 1}")

        # Start writer first
        writer_thread = threading.Thread(target=writer, args=(0,))
        writer_thread.start()

        # Start readers
        reader_threads = []
        for i in range(3):
            t = threading.Thread(target=reader, args=(i,))
            reader_threads.append(t)
            t.start()

        writer_thread.join()
        for t in reader_threads:
            t.join()

        # Writer should complete before readers
        assert operations[0].startswith("W")

        # Cleanup
        hp_redis_client.delete(value_key)


@pytest.mark.integration
@pytest.mark.skipif(not LOCKS_AVAILABLE, reason="Lock components not available")
class TestSemaphore:
    """Test distributed semaphore."""

    def test_semaphore_limit(self, hp_redis_client, unique_key):
        """Test that semaphore enforces limit."""
        semaphore_key = f"{unique_key}:semaphore"
        limit = 3

        semaphore = Semaphore(hp_redis_client, semaphore_key, limit=limit)

        acquired_count = []
        lock_obj = threading.Lock()

        def worker(worker_id):
            acquired = semaphore.acquire(timeout=5)
            if acquired:
                with lock_obj:
                    acquired_count.append(worker_id)

                time.sleep(0.5)  # Simulate work

                with lock_obj:
                    acquired_count.remove(worker_id)

                semaphore.release()

        # Start more workers than limit
        threads = []
        for i in range(10):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Check that only 'limit' workers are active at once
        time.sleep(0.2)
        with lock_obj:
            assert len(acquired_count) <= limit

        for t in threads:
            t.join(timeout=10)

    def test_semaphore_release(self, hp_redis_client, unique_key):
        """Test semaphore acquire and release."""
        semaphore_key = f"{unique_key}:semaphore"
        semaphore = Semaphore(hp_redis_client, semaphore_key, limit=2)

        # Acquire twice
        acquired1 = semaphore.acquire(timeout=1)
        acquired2 = semaphore.acquire(timeout=1)
        assert acquired1 is True
        assert acquired2 is True

        # Third should fail
        acquired3 = semaphore.acquire(timeout=0.5)
        assert acquired3 is False

        # Release one
        semaphore.release()

        # Now should be able to acquire
        acquired4 = semaphore.acquire(timeout=1)
        assert acquired4 is True

        # Cleanup
        semaphore.release()
        semaphore.release()


@pytest.mark.integration
@pytest.mark.skipif(not LOCKS_AVAILABLE, reason="Lock components not available")
class TestLockPatterns:
    """Test common distributed lock patterns."""

    def test_lock_with_renewal(self, hp_redis_client, unique_key):
        """Test lock with automatic renewal."""
        lock_key = f"{unique_key}:renewable"
        lock = DistributedLock(hp_redis_client, lock_key, ttl_ms=1000)

        acquired = lock.try_acquire(blocking=False)
        assert acquired is True

        # Renew lock multiple times
        for _ in range(5):
            time.sleep(0.5)
            extended = lock.extend(ttl_ms=1000)
            assert extended is True

        # Lock should still be held
        assert hp_redis_client.exists(lock_key) == 1

        lock.release()

    def test_try_lock_pattern(self, hp_redis_client, unique_key):
        """Test try-lock pattern (non-blocking)."""
        lock_key = f"{unique_key}:trylock"
        lock = DistributedLock(hp_redis_client, lock_key)

        # First acquire succeeds
        acquired1 = lock.try_acquire(blocking=False)
        assert acquired1 is True

        # Immediate second acquire fails
        lock2 = DistributedLock(hp_redis_client, lock_key)
        acquired2 = lock2.try_acquire(blocking=False)
        assert acquired2 is False

        lock.release()

    def test_lock_with_work_timeout(self, hp_redis_client, unique_key):
        """Test lock for work with maximum duration."""
        lock_key = f"{unique_key}:work"
        work_timeout_ms = 2000

        lock = DistributedLock(hp_redis_client, lock_key, ttl_ms=work_timeout_ms)

        start_time = time.time()
        acquired = lock.try_acquire(blocking=True, timeout=5)

        if acquired:
            try:
                # Simulate work that might take variable time
                time.sleep(0.5)

                # Check if we're still within timeout
                elapsed = (time.time() - start_time) * 1000
                assert elapsed < work_timeout_ms

            finally:
                lock.release()
