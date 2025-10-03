"""
Integration tests for concurrent/threaded Redis operations.

Tests thread safety and concurrent access patterns including:
- Concurrent reads and writes
- Thread pool operations
- Race conditions
- Atomic operations under load
- Connection pool behavior
"""

import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List


@pytest.mark.integration
class TestConcurrentReads:
    """Test concurrent read operations."""

    def test_concurrent_get(self, redis_client, unique_key):
        """Test concurrent GET operations."""
        key = f"{unique_key}:concurrent"
        value = "test_value"
        redis_client.set(key, value)

        results = []
        errors = []

        def read_value():
            try:
                result = redis_client.get(key)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Run 50 concurrent reads
        threads = []
        for _ in range(50):
            t = threading.Thread(target=read_value)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All reads should succeed with correct value
        assert len(errors) == 0
        assert all(r == value for r in results)
        assert len(results) == 50

        # Cleanup
        redis_client.delete(key)

    def test_concurrent_mget(self, redis_client, unique_key):
        """Test concurrent MGET operations."""
        keys = [f"{unique_key}:multi:{i}" for i in range(10)]
        values = [f"value_{i}" for i in range(10)]

        # Set up data
        mapping = {k: v for k, v in zip(keys, values)}
        redis_client.mset(mapping)

        results = []
        errors = []

        def read_multiple():
            try:
                result = redis_client.mget(keys)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Run concurrent multi-gets
        threads = []
        for _ in range(20):
            t = threading.Thread(target=read_multiple)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All reads should succeed
        assert len(errors) == 0
        assert all(r == values for r in results)

        # Cleanup
        redis_client.delete(*keys)


@pytest.mark.integration
class TestConcurrentWrites:
    """Test concurrent write operations."""

    def test_concurrent_set(self, redis_client, unique_key):
        """Test concurrent SET operations on different keys."""
        base_key = f"{unique_key}:concurrent"
        num_threads = 50

        errors = []

        def write_value(index):
            try:
                key = f"{base_key}:{index}"
                redis_client.set(key, f"value_{index}")
            except Exception as e:
                errors.append(e)

        # Run concurrent writes
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=write_value, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All writes should succeed
        assert len(errors) == 0

        # Verify all values
        for i in range(num_threads):
            key = f"{base_key}:{i}"
            assert redis_client.get(key) == f"value_{i}"

        # Cleanup
        keys = [f"{base_key}:{i}" for i in range(num_threads)]
        redis_client.delete(*keys)

    def test_concurrent_incr(self, redis_client, unique_key):
        """Test concurrent INCR operations (atomic counter)."""
        key = f"{unique_key}:counter"
        num_threads = 100
        increments_per_thread = 10

        errors = []

        def increment_counter():
            try:
                for _ in range(increments_per_thread):
                    redis_client.incr(key)
            except Exception as e:
                errors.append(e)

        # Run concurrent increments
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=increment_counter)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All increments should succeed
        assert len(errors) == 0

        # Final count should be exactly num_threads * increments_per_thread
        final_count = int(redis_client.get(key))
        assert final_count == num_threads * increments_per_thread

        # Cleanup
        redis_client.delete(key)

    def test_concurrent_list_operations(self, redis_client, unique_key):
        """Test concurrent list LPUSH operations."""
        key = f"{unique_key}:list"
        num_threads = 50

        errors = []

        def push_items(index):
            try:
                for i in range(10):
                    redis_client.lpush(key, f"thread_{index}_item_{i}")
            except Exception as e:
                errors.append(e)

        # Run concurrent pushes
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=push_items, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All pushes should succeed
        assert len(errors) == 0

        # Total items should be num_threads * 10
        assert redis_client.llen(key) == num_threads * 10

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestThreadPoolOperations:
    """Test operations using thread pools."""

    def test_thread_pool_reads(self, redis_client, unique_key):
        """Test thread pool for read operations."""
        # Set up test data
        keys = [f"{unique_key}:pool:{i}" for i in range(100)]
        mapping = {k: f"value_{i}" for i, k in enumerate(keys)}
        redis_client.mset(mapping)

        def read_key(key):
            return redis_client.get(key)

        # Execute reads in thread pool
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(read_key, key) for key in keys]
            results = [future.result() for future in as_completed(futures)]

        # All reads should succeed
        assert len(results) == 100
        assert all(r.startswith("value_") for r in results)

        # Cleanup
        redis_client.delete(*keys)

    def test_thread_pool_writes(self, redis_client, unique_key):
        """Test thread pool for write operations."""
        base_key = f"{unique_key}:pool"

        def write_key(index):
            key = f"{base_key}:{index}"
            redis_client.set(key, f"value_{index}")
            return key

        # Execute writes in thread pool
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(write_key, i) for i in range(100)]
            keys = [future.result() for future in as_completed(futures)]

        # Verify all writes
        assert len(keys) == 100
        for i, key in enumerate(keys):
            value = redis_client.get(key)
            assert value.startswith("value_")

        # Cleanup
        redis_client.delete(*keys)

    def test_thread_pool_mixed_operations(self, redis_client, unique_key):
        """Test thread pool with mixed read/write operations."""
        key = f"{unique_key}:mixed"
        redis_client.set(key, "0")

        def increment_and_read():
            redis_client.incr(key)
            return int(redis_client.get(key))

        # Execute mixed operations in thread pool
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(increment_and_read) for _ in range(100)]
            results = [future.result() for future in as_completed(futures)]

        # All operations should succeed
        assert len(results) == 100

        # Final value should be 100
        assert int(redis_client.get(key)) == 100

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestRaceConditions:
    """Test handling of race conditions."""

    def test_check_and_set_race(self, redis_client, unique_key):
        """Test check-and-set pattern with potential race condition."""
        key = f"{unique_key}:race"
        redis_client.set(key, "0")

        successful_updates = []
        errors = []

        def conditional_update():
            """Update only if current value is even."""
            try:
                # This has a race condition without WATCH
                current = int(redis_client.get(key))
                if current % 2 == 0:
                    time.sleep(0.001)  # Simulate processing
                    redis_client.set(key, str(current + 1))
                    successful_updates.append(current)
            except Exception as e:
                errors.append(e)

        # Run concurrent conditional updates
        threads = []
        for _ in range(20):
            t = threading.Thread(target=conditional_update)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # No errors should occur
        assert len(errors) == 0

        # Some updates should succeed (demonstrates race condition)
        assert len(successful_updates) > 0

        # Cleanup
        redis_client.delete(key)

    def test_watch_prevents_race(self, redis_client, unique_key):
        """Test that WATCH prevents race conditions."""
        key = f"{unique_key}:watch_race"
        redis_client.set(key, "0")

        successful_updates = []
        failed_updates = []
        errors = []

        def atomic_conditional_update():
            """Atomic update using WATCH."""
            try:
                pipe = redis_client.pipeline()
                pipe.watch(key)

                current = int(pipe.get(key))
                if current % 2 == 0:
                    time.sleep(0.001)  # Simulate processing

                    pipe.multi()
                    pipe.set(key, str(current + 1))
                    try:
                        pipe.execute()
                        successful_updates.append(current)
                    except Exception:
                        failed_updates.append(current)
                else:
                    pipe.unwatch()
            except Exception as e:
                errors.append(e)

        # Run concurrent atomic updates
        threads = []
        for _ in range(20):
            t = threading.Thread(target=atomic_conditional_update)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # No errors should occur
        assert len(errors) == 0

        # Final value should be consistent
        final = int(redis_client.get(key))
        assert final > 0

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestConnectionPoolBehavior:
    """Test connection pool behavior under concurrent load."""

    def test_pool_connection_reuse(self, redis_client, unique_key):
        """Test that connections are properly reused from pool."""
        key = f"{unique_key}:pool_test"

        def make_requests(thread_id):
            """Make multiple requests from same thread."""
            for i in range(10):
                redis_client.set(f"{key}:{thread_id}:{i}", f"value_{i}")
                redis_client.get(f"{key}:{thread_id}:{i}")

        # Run multiple threads
        threads = []
        for i in range(10):
            t = threading.Thread(target=make_requests, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Verify data integrity
        for thread_id in range(10):
            for i in range(10):
                key_name = f"{key}:{thread_id}:{i}"
                assert redis_client.get(key_name) == f"value_{i}"

        # Cleanup
        keys = [f"{key}:{tid}:{i}" for tid in range(10) for i in range(10)]
        redis_client.delete(*keys)

    def test_pool_exhaustion_handling(self, redis_client, unique_key):
        """Test behavior when connection pool is under heavy load."""
        key = f"{unique_key}:exhaustion"

        results = []
        errors = []

        def long_running_operation(index):
            try:
                # Simulate long operation with pipeline
                pipe = redis_client.pipeline()
                for i in range(10):
                    pipe.set(f"{key}:{index}:{i}", f"value_{i}")
                pipe.execute()
                results.append(index)
            except Exception as e:
                errors.append(e)

        # Run many concurrent operations
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(long_running_operation, i) for i in range(100)]
            for future in as_completed(futures):
                future.result()

        # All operations should complete
        assert len(errors) == 0
        assert len(results) == 100

        # Cleanup
        keys = [f"{key}:{idx}:{i}" for idx in range(100) for i in range(10)]
        redis_client.delete(*keys)


@pytest.mark.integration
@pytest.mark.slow
class TestHighConcurrencyScenarios:
    """Test high concurrency scenarios."""

    def test_thousand_concurrent_operations(self, redis_client, unique_key):
        """Test 1000 concurrent operations."""
        base_key = f"{unique_key}:high_concurrency"

        def mixed_operation(index):
            key = f"{base_key}:{index}"
            # Write
            redis_client.set(key, f"value_{index}")
            # Read
            value = redis_client.get(key)
            # Increment a shared counter
            redis_client.incr(f"{base_key}:counter")
            return value == f"value_{index}"

        # Execute 1000 operations
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(mixed_operation, i) for i in range(1000)]
            results = [future.result() for future in as_completed(futures)]

        # All should succeed
        assert all(results)

        # Counter should be exactly 1000
        assert int(redis_client.get(f"{base_key}:counter")) == 1000

        # Cleanup
        keys = [f"{base_key}:{i}" for i in range(1000)]
        keys.append(f"{base_key}:counter")
        redis_client.delete(*keys)
