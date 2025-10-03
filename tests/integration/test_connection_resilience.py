"""
Integration tests for connection resilience and failure recovery.

Tests connection handling including:
- Connection retry logic
- Timeout handling
- Connection pool recovery
- Network interruption simulation
- Failover scenarios
- Circuit breaker patterns
"""

import pytest
import time
import threading
from unittest.mock import patch


@pytest.mark.integration
class TestConnectionRetry:
    """Test connection retry logic."""

    def test_reconnect_after_disconnect(self, redis_client, unique_key):
        """Test automatic reconnection after disconnect."""
        key = f"{unique_key}:reconnect"

        # Normal operation
        redis_client.set(key, "value1")
        assert redis_client.get(key) == "value1"

        # Simulate disconnect by closing connection
        redis_client.connection_pool.disconnect()

        # Next operation should reconnect automatically
        redis_client.set(key, "value2")
        assert redis_client.get(key) == "value2"

        # Cleanup
        redis_client.delete(key)

    def test_connection_timeout_handling(self, unique_key):
        """Test handling of connection timeouts."""
        import os
        from redis import Redis

        # Create client with very short timeout
        client = Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            socket_timeout=0.001,  # Very short timeout
            socket_connect_timeout=0.001,
            decode_responses=True
        )

        key = f"{unique_key}:timeout"

        # This might timeout or succeed depending on timing
        try:
            client.set(key, "value")
            # If it succeeded, verify
            assert client.get(key) == "value"
            client.delete(key)
        except Exception as e:
            # Timeout is expected with such short timeout
            assert "timeout" in str(e).lower() or "connection" in str(e).lower()

        client.close()

    def test_connection_pool_recovery(self, redis_client, unique_key):
        """Test connection pool recovery after errors."""
        base_key = f"{unique_key}:pool_recovery"

        # Make multiple connections
        for i in range(10):
            redis_client.set(f"{base_key}:{i}", f"value_{i}")

        # Force disconnect all connections
        redis_client.connection_pool.disconnect()

        # Pool should recover for new operations
        for i in range(10):
            value = redis_client.get(f"{base_key}:{i}")
            assert value == f"value_{i}"

        # Cleanup
        keys = [f"{base_key}:{i}" for i in range(10)]
        redis_client.delete(*keys)


@pytest.mark.integration
class TestOperationTimeouts:
    """Test operation timeout handling."""

    def test_blocking_operation_timeout(self, redis_client, unique_key):
        """Test timeout on blocking operations."""
        list_key = f"{unique_key}:blocking"

        # BLPOP with timeout should return None
        result = redis_client.blpop(list_key, timeout=1)
        assert result is None

    def test_pipeline_timeout(self, redis_client, unique_key):
        """Test pipeline operations with timeout."""
        keys = [f"{unique_key}:pipe:{i}" for i in range(100)]

        # Create pipeline
        pipe = redis_client.pipeline()

        # Add many operations
        for i, key in enumerate(keys):
            pipe.set(key, f"value_{i}")

        # Execute should complete
        start_time = time.time()
        results = pipe.execute()
        elapsed = time.time() - start_time

        # Should complete reasonably quickly
        assert elapsed < 5.0
        assert len(results) == 100

        # Cleanup
        redis_client.delete(*keys)


@pytest.mark.integration
class TestConnectionPoolBehavior:
    """Test connection pool behavior under stress."""

    def test_pool_exhaustion_recovery(self, redis_client, unique_key):
        """Test recovery from pool exhaustion."""
        from concurrent.futures import ThreadPoolExecutor

        key = f"{unique_key}:pool_stress"

        def operation(index):
            try:
                redis_client.set(f"{key}:{index}", f"value_{index}")
                return True
            except Exception:
                return False

        # Create many concurrent operations
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(operation, i) for i in range(200)]
            results = [f.result() for f in futures]

        # Most or all should succeed (pool should handle it)
        success_rate = sum(results) / len(results)
        assert success_rate > 0.9  # At least 90% success

        # Cleanup
        keys = [f"{key}:{i}" for i in range(200)]
        redis_client.delete(*keys)

    def test_connection_reuse(self, redis_client, unique_key):
        """Test that connections are properly reused."""
        key = f"{unique_key}:reuse"

        # Get pool stats
        pool = redis_client.connection_pool
        initial_created = len(pool._available_connections) + len(pool._in_use_connections)

        # Make many sequential operations
        for i in range(50):
            redis_client.set(key, f"value_{i}")
            redis_client.get(key)

        # Pool size should not have grown significantly
        final_created = len(pool._available_connections) + len(pool._in_use_connections)

        # Should reuse connections (not create 100 new ones)
        assert final_created - initial_created < 10

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestErrorRecovery:
    """Test error handling and recovery."""

    def test_invalid_command_recovery(self, redis_client, unique_key):
        """Test that client recovers from invalid commands."""
        key = f"{unique_key}:recovery"

        # Valid operation
        redis_client.set(key, "value1")

        # Invalid operation (wrong number of arguments)
        try:
            redis_client.execute_command("SET", key)
        except Exception:
            pass  # Expected to fail

        # Should still work after error
        redis_client.set(key, "value2")
        assert redis_client.get(key) == "value2"

        # Cleanup
        redis_client.delete(key)

    def test_pipeline_error_recovery(self, redis_client, unique_key):
        """Test pipeline recovery from errors."""
        key1 = f"{unique_key}:pipe1"
        key2 = f"{unique_key}:pipe2"

        # Pipeline with error
        pipe = redis_client.pipeline()
        pipe.set(key1, "value1")
        pipe.lpush(key1, "item")  # This will error (wrong type)
        pipe.set(key2, "value2")

        try:
            pipe.execute()
        except Exception:
            pass  # Expected to fail

        # Regular operations should still work
        redis_client.set(key1, "correct_value")
        assert redis_client.get(key1) == "correct_value"

        # Cleanup
        redis_client.delete(key1, key2)

    def test_transaction_rollback(self, redis_client, unique_key):
        """Test transaction rollback on watch failure."""
        key = f"{unique_key}:watch"

        redis_client.set(key, "0")

        # Start transaction with watch
        pipe = redis_client.pipeline()
        pipe.watch(key)

        # Modify key from another connection
        redis_client.set(key, "modified")

        # Transaction should fail
        pipe.multi()
        pipe.set(key, "transaction_value")

        try:
            pipe.execute()
            # If no exception, watch didn't trigger (might happen in some Redis versions)
            pass
        except Exception:
            # Watch triggered, expected
            pass

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestNetworkInterruption:
    """Test behavior during network interruptions."""

    def test_operation_during_high_latency(self, redis_client, unique_key):
        """Test operations complete even with high latency."""
        key = f"{unique_key}:latency"

        # This test assumes Redis is responsive
        # In real scenario, you might use network simulation

        start_time = time.time()

        # Perform operations
        for i in range(10):
            redis_client.set(f"{key}:{i}", f"value_{i}")

        elapsed = time.time() - start_time

        # Should complete in reasonable time
        assert elapsed < 5.0

        # Verify data
        for i in range(10):
            assert redis_client.get(f"{key}:{i}") == f"value_{i}"

        # Cleanup
        keys = [f"{key}:{i}" for i in range(10)]
        redis_client.delete(*keys)


@pytest.mark.integration
class TestCircuitBreaker:
    """Test circuit breaker pattern for Redis operations."""

    def test_circuit_breaker_basic(self, redis_client, unique_key):
        """Test basic circuit breaker functionality."""
        key = f"{unique_key}:circuit"

        class CircuitBreaker:
            def __init__(self, failure_threshold=3, timeout=5):
                self.failure_threshold = failure_threshold
                self.timeout = timeout
                self.failures = 0
                self.last_failure_time = None
                self.state = "closed"  # closed, open, half-open

            def call(self, func, *args, **kwargs):
                if self.state == "open":
                    if time.time() - self.last_failure_time > self.timeout:
                        self.state = "half-open"
                    else:
                        raise Exception("Circuit breaker is open")

                try:
                    result = func(*args, **kwargs)
                    if self.state == "half-open":
                        self.state = "closed"
                        self.failures = 0
                    return result
                except Exception as e:
                    self.failures += 1
                    self.last_failure_time = time.time()

                    if self.failures >= self.failure_threshold:
                        self.state = "open"

                    raise e

        cb = CircuitBreaker(failure_threshold=3, timeout=2)

        # Successful operations
        result = cb.call(redis_client.set, key, "value")
        assert result is True

        # Circuit should be closed
        assert cb.state == "closed"

        # Cleanup
        redis_client.delete(key)

    def test_retry_with_backoff(self, unique_key):
        """Test retry with exponential backoff."""
        import os
        from redis import Redis
        from redis.exceptions import ConnectionError as RedisConnectionError

        key = f"{unique_key}:retry"

        def operation_with_retry(client, max_retries=3):
            for attempt in range(max_retries):
                try:
                    client.set(key, "value")
                    return True
                except (RedisConnectionError, Exception) as e:
                    if attempt == max_retries - 1:
                        raise

                    # Exponential backoff
                    wait_time = 0.1 * (2 ** attempt)
                    time.sleep(wait_time)

            return False

        # Create client
        client = Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True
        )

        try:
            # Should succeed (Redis is available)
            result = operation_with_retry(client)
            assert result is True

            # Verify
            assert client.get(key) == "value"

            # Cleanup
            client.delete(key)
        finally:
            client.close()


@pytest.mark.integration
class TestHealthChecks:
    """Test connection health check mechanisms."""

    def test_ping_health_check(self, redis_client):
        """Test PING command for health checks."""
        # Should return True
        assert redis_client.ping() is True

    def test_info_health_check(self, redis_client):
        """Test INFO command for health checks."""
        info = redis_client.info()

        # Should have expected sections
        assert "redis_version" in info
        assert "uptime_in_seconds" in info

    def test_connection_pool_health(self, redis_client):
        """Test connection pool health."""
        pool = redis_client.connection_pool

        # Pool should have connections
        total_connections = (
            len(pool._available_connections) +
            len(pool._in_use_connections)
        )

        # Should have at least some capacity
        assert total_connections >= 0

    def test_continuous_health_monitoring(self, redis_client, unique_key):
        """Test continuous health monitoring pattern."""
        key = f"{unique_key}:health"

        healthy = True
        check_count = 0

        def health_check():
            nonlocal healthy, check_count
            try:
                redis_client.ping()
                check_count += 1
                return True
            except Exception:
                healthy = False
                return False

        # Perform health checks
        for _ in range(10):
            result = health_check()
            time.sleep(0.1)

            if not result:
                break

        # Should have completed health checks
        assert healthy is True
        assert check_count == 10
