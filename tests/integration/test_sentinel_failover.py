"""
Integration tests for Redis Sentinel failover.

Tests Sentinel functionality including:
- Sentinel connection and discovery
- Master/replica discovery
- Automatic failover
- Connection switching
- Sentinel monitoring
"""

import pytest
import time


@pytest.mark.integration
@pytest.mark.sentinel
class TestSentinelConnection:
    """Test Redis Sentinel connection."""

    def test_sentinel_connection(self, redis_sentinel_client):
        """Test connection to Redis via Sentinel."""
        # Should be able to ping
        assert redis_sentinel_client.ping() is True

    def test_sentinel_master_discovery(self, redis_sentinel_client, unique_key):
        """Test that Sentinel can discover master."""
        key = f"{unique_key}:master"

        # Should be able to write to master
        redis_sentinel_client.set(key, "master_value")

        # Should be able to read
        assert redis_sentinel_client.get(key) == "master_value"

        # Cleanup
        redis_sentinel_client.delete(key)


@pytest.mark.integration
@pytest.mark.sentinel
class TestSentinelOperations:
    """Test basic operations through Sentinel."""

    def test_read_write_operations(self, redis_sentinel_client, unique_key):
        """Test read/write operations through Sentinel."""
        key = f"{unique_key}:rw"

        # Write
        redis_sentinel_client.set(key, "value")

        # Read
        assert redis_sentinel_client.get(key) == "value"

        # Update
        redis_sentinel_client.set(key, "updated_value")
        assert redis_sentinel_client.get(key) == "updated_value"

        # Cleanup
        redis_sentinel_client.delete(key)

    def test_concurrent_operations(self, redis_sentinel_client, unique_key):
        """Test concurrent operations through Sentinel."""
        from concurrent.futures import ThreadPoolExecutor

        base_key = f"{unique_key}:concurrent"

        def operation(index):
            key = f"{base_key}:{index}"
            redis_sentinel_client.set(key, f"value_{index}")
            return redis_sentinel_client.get(key) == f"value_{index}"

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(operation, i) for i in range(50)]
            results = [f.result() for f in futures]

        # All should succeed
        assert all(results)

        # Cleanup
        keys = [f"{base_key}:{i}" for i in range(50)]
        redis_sentinel_client.delete(*keys)


@pytest.mark.integration
@pytest.mark.sentinel
class TestSentinelResilience:
    """Test Sentinel resilience features."""

    def test_continuous_operations(self, redis_sentinel_client, unique_key):
        """Test continuous operations through Sentinel."""
        base_key = f"{unique_key}:continuous"

        # Perform operations over time
        for i in range(20):
            key = f"{base_key}:{i}"
            redis_sentinel_client.set(key, f"value_{i}")

            # Small delay between operations
            time.sleep(0.1)

            # Verify
            assert redis_sentinel_client.get(key) == f"value_{i}"

        # Cleanup
        keys = [f"{base_key}:{i}" for i in range(20)]
        redis_sentinel_client.delete(*keys)

    def test_connection_recovery(self, redis_sentinel_client, unique_key):
        """Test connection recovery through Sentinel."""
        key = f"{unique_key}:recovery"

        # Normal operation
        redis_sentinel_client.set(key, "value1")

        # Disconnect
        try:
            redis_sentinel_client.connection_pool.disconnect()
        except Exception:
            pass

        # Should reconnect automatically
        redis_sentinel_client.set(key, "value2")
        assert redis_sentinel_client.get(key) == "value2"

        # Cleanup
        redis_sentinel_client.delete(key)


@pytest.mark.integration
@pytest.mark.sentinel
@pytest.mark.slow
class TestSentinelFailover:
    """Test Sentinel failover scenarios."""

    def test_operations_during_failover(self, redis_sentinel_client, unique_key):
        """Test that operations can continue during/after failover."""
        # Note: This test cannot actually trigger failover without cluster control
        # It tests resilience to connection issues

        base_key = f"{unique_key}:failover"

        # Write data before
        for i in range(10):
            key = f"{base_key}:before:{i}"
            redis_sentinel_client.set(key, f"value_{i}")

        # In a real failover scenario, Sentinel would switch to new master
        # Here we just verify operations continue

        # Write data after
        for i in range(10):
            key = f"{base_key}:after:{i}"
            redis_sentinel_client.set(key, f"value_{i}")

        # Verify all data
        for i in range(10):
            assert redis_sentinel_client.get(f"{base_key}:before:{i}") == f"value_{i}"
            assert redis_sentinel_client.get(f"{base_key}:after:{i}") == f"value_{i}"

        # Cleanup
        keys = (
            [f"{base_key}:before:{i}" for i in range(10)] +
            [f"{base_key}:after:{i}" for i in range(10)]
        )
        redis_sentinel_client.delete(*keys)


@pytest.mark.integration
@pytest.mark.sentinel
class TestSentinelMonitoring:
    """Test Sentinel monitoring capabilities."""

    def test_sentinel_health_check(self, redis_sentinel_client):
        """Test health check through Sentinel."""
        # Ping should work
        assert redis_sentinel_client.ping() is True

    def test_sentinel_info(self, redis_sentinel_client):
        """Test getting info through Sentinel."""
        info = redis_sentinel_client.info()

        # Should have basic info sections
        assert "redis_version" in info
        assert "role" in info

    def test_continuous_monitoring(self, redis_sentinel_client, unique_key):
        """Test continuous monitoring pattern."""
        key = f"{unique_key}:monitor"

        # Perform operations while monitoring
        for i in range(10):
            # Operation
            redis_sentinel_client.set(key, f"value_{i}")

            # Health check
            assert redis_sentinel_client.ping() is True

            time.sleep(0.2)

        # Cleanup
        redis_sentinel_client.delete(key)


@pytest.mark.integration
@pytest.mark.sentinel
class TestSentinelPipeline:
    """Test pipeline operations through Sentinel."""

    def test_pipeline_basic(self, redis_sentinel_client, unique_key):
        """Test basic pipeline through Sentinel."""
        keys = [f"{unique_key}:pipe:{i}" for i in range(10)]

        # Pipeline
        pipe = redis_sentinel_client.pipeline()

        for i, key in enumerate(keys):
            pipe.set(key, f"value_{i}")

        results = pipe.execute()

        # All should succeed
        assert all(r is True for r in results)

        # Verify
        for i, key in enumerate(keys):
            assert redis_sentinel_client.get(key) == f"value_{i}"

        # Cleanup
        redis_sentinel_client.delete(*keys)

    def test_pipeline_mixed_operations(self, redis_sentinel_client, unique_key):
        """Test pipeline with mixed operations through Sentinel."""
        key1 = f"{unique_key}:p1"
        key2 = f"{unique_key}:p2"
        key3 = f"{unique_key}:p3"

        # Setup
        redis_sentinel_client.set(key1, "10")

        # Pipeline
        pipe = redis_sentinel_client.pipeline()
        pipe.incr(key1)
        pipe.set(key2, "value2")
        pipe.lpush(key3, "item1", "item2")
        pipe.lrange(key3, 0, -1)

        results = pipe.execute()

        # Check results
        assert results[0] == 11  # incr
        assert results[1] is True  # set
        assert results[2] == 2  # lpush
        assert results[3] == ["item2", "item1"]  # lrange

        # Cleanup
        redis_sentinel_client.delete(key1, key2, key3)


@pytest.mark.integration
@pytest.mark.sentinel
class TestSentinelTransactions:
    """Test transactions through Sentinel."""

    def test_transaction_basic(self, redis_sentinel_client, unique_key):
        """Test basic transaction through Sentinel."""
        key = f"{unique_key}:trans"

        redis_sentinel_client.set(key, "0")

        # Transaction
        pipe = redis_sentinel_client.pipeline()
        pipe.multi()
        pipe.incr(key)
        pipe.incr(key)
        pipe.get(key)

        results = pipe.execute()

        # Check results
        assert results == [1, 2, "2"]

        # Cleanup
        redis_sentinel_client.delete(key)

    def test_watch_transaction(self, redis_sentinel_client, unique_key):
        """Test WATCH transaction through Sentinel."""
        key = f"{unique_key}:watch"

        redis_sentinel_client.set(key, "0")

        # Watch and transaction
        pipe = redis_sentinel_client.pipeline()
        pipe.watch(key)

        current = int(redis_sentinel_client.get(key))

        pipe.multi()
        pipe.set(key, str(current + 1))

        results = pipe.execute()

        # Should succeed
        assert results == [True]

        # Verify
        assert redis_sentinel_client.get(key) == "1"

        # Cleanup
        redis_sentinel_client.delete(key)


@pytest.mark.integration
@pytest.mark.sentinel
class TestSentinelPubSub:
    """Test pub/sub through Sentinel."""

    def test_pubsub_basic(self, redis_sentinel_client, unique_key):
        """Test basic pub/sub through Sentinel."""
        import threading

        channel = f"{unique_key}:channel"
        messages = []

        def subscriber():
            pubsub = redis_sentinel_client.pubsub()
            pubsub.subscribe(channel)

            for message in pubsub.listen():
                if message['type'] == 'message':
                    messages.append(message['data'])
                    if len(messages) == 3:
                        break

            pubsub.unsubscribe(channel)
            pubsub.close()

        # Start subscriber
        thread = threading.Thread(target=subscriber)
        thread.start()

        # Wait for subscription
        time.sleep(0.5)

        # Publish messages
        for i in range(3):
            redis_sentinel_client.publish(channel, f"message_{i}")
            time.sleep(0.1)

        # Wait for subscriber
        thread.join(timeout=5)

        # Should receive all messages
        assert len(messages) == 3
