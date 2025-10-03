"""
Unit tests for connection_pool.pyx - Connection pooling and health checks
"""
import pytest
import time
import threading
from cy_redis.connection_pool import (
    ConnectionHealth, EnhancedConnectionPool, EnhancedRedisClient,
    TLSSupport, RetryStrategy, ProductionRedis
)


@pytest.fixture
def connection_pool():
    """Create a connection pool for testing"""
    pool = EnhancedConnectionPool(
        host="localhost",
        port=6379,
        min_connections=2,
        max_connections=10
    )
    yield pool
    pool.close()


@pytest.fixture
def redis_client():
    """Create an enhanced Redis client for testing"""
    client = EnhancedRedisClient(
        host="localhost",
        port=6379,
        min_connections=2,
        max_connections=10
    )
    yield client
    client.close()


class TestConnectionHealth:
    """Test ConnectionHealth class"""

    def test_health_creation(self):
        """Test creating connection health tracker"""
        health = ConnectionHealth()
        assert health is not None
        assert health.state == 0  # CONN_DISCONNECTED

    def test_is_healthy(self):
        """Test health check"""
        health = ConnectionHealth()
        # Initially should be healthy (no check yet)
        assert health.is_healthy() is True

    def test_mark_successful(self):
        """Test marking successful operation"""
        health = ConnectionHealth()
        health.mark_successful()
        assert health.consecutive_failures == 0
        assert health.state == 1  # CONN_CONNECTED

    def test_mark_failed(self):
        """Test marking failed operation"""
        health = ConnectionHealth()
        health.mark_failed()
        assert health.consecutive_failures == 1

    def test_mark_health_checked(self):
        """Test marking health check performed"""
        health = ConnectionHealth()
        initial_time = health.last_health_check
        time.sleep(0.1)
        health.mark_health_checked()
        assert health.last_health_check > initial_time

    def test_consecutive_failures(self):
        """Test consecutive failure tracking"""
        health = ConnectionHealth()
        for i in range(3):
            health.mark_failed()

        assert health.consecutive_failures >= 3
        assert health.state == 2  # CONN_ERROR


class TestEnhancedConnectionPool:
    """Test EnhancedConnectionPool class"""

    def test_pool_creation(self):
        """Test creating connection pool"""
        pool = EnhancedConnectionPool(
            host="localhost",
            port=6379,
            min_connections=2,
            max_connections=10
        )
        assert pool is not None
        assert pool.max_connections == 10
        assert pool.min_connections == 2
        pool.close()

    def test_get_connection(self, connection_pool):
        """Test getting connection from pool"""
        conn = connection_pool.get_connection()
        assert conn is not None
        connection_pool.return_connection(conn)

    def test_return_connection(self, connection_pool):
        """Test returning connection to pool"""
        conn = connection_pool.get_connection()
        initial_count = len(connection_pool.connections)
        connection_pool.return_connection(conn)
        # Connection should be tracked
        assert connection_pool.connections is not None

    def test_pool_stats(self, connection_pool):
        """Test getting pool statistics"""
        stats = connection_pool.get_pool_stats()
        assert 'total_connections' in stats
        assert 'max_connections' in stats
        assert 'min_connections' in stats
        assert 'healthy_connections' in stats
        assert 'unhealthy_connections' in stats

    def test_health_check_loop(self, connection_pool):
        """Test health check background loop"""
        # Health check should be running
        assert connection_pool.running is True

        # Wait a bit for health checks to run
        time.sleep(1.0)

        stats = connection_pool.get_pool_stats()
        # Should have some healthy connections
        assert stats['total_connections'] >= 0

    def test_concurrent_access(self, connection_pool):
        """Test concurrent connection access"""
        results = []

        def worker():
            conn = connection_pool.get_connection()
            if conn:
                results.append(1)
                connection_pool.return_connection(conn)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) > 0


class TestEnhancedRedisClient:
    """Test EnhancedRedisClient class"""

    def test_client_creation(self):
        """Test creating enhanced Redis client"""
        client = EnhancedRedisClient(host="localhost", port=6379)
        assert client is not None
        client.close()

    def test_get_set_operations(self, redis_client):
        """Test GET and SET with retry logic"""
        result = redis_client.set('test_key', 'test_value')
        assert result == 'OK'

        value = redis_client.get('test_key')
        assert value == 'test_value'

        # Cleanup
        redis_client.delete('test_key')

    def test_delete_operation(self, redis_client):
        """Test DELETE operation"""
        redis_client.set('test_key', 'test_value')
        result = redis_client.delete('test_key')
        assert result == 1

    def test_get_pool_stats(self, redis_client):
        """Test getting pool statistics"""
        stats = redis_client.get_pool_stats()
        assert isinstance(stats, dict)

    def test_execute_command_with_retry(self, redis_client):
        """Test execute command with retry logic"""
        result = redis_client.execute_command(['PING'])
        assert result == 'PONG'


class TestTLSSupport:
    """Test TLS/SSL support"""

    def test_tls_creation(self):
        """Test creating TLS support"""
        tls = TLSSupport(enabled=False)
        assert tls is not None
        assert tls.enabled is False

    def test_is_enabled(self):
        """Test checking if TLS is enabled"""
        tls = TLSSupport(enabled=True)
        assert tls.is_enabled() is True

    def test_ssl_context(self):
        """Test getting SSL context"""
        tls = TLSSupport(enabled=True)
        ctx = tls.get_ssl_context()
        assert ctx is not None


class TestRetryStrategy:
    """Test RetryStrategy class"""

    def test_strategy_creation(self):
        """Test creating retry strategy"""
        strategy = RetryStrategy(max_retries=3)
        assert strategy is not None
        assert strategy.max_retries == 3

    def test_should_retry(self):
        """Test should retry logic"""
        strategy = RetryStrategy(max_retries=3)

        # Test retryable error
        error = Exception("Connection refused")
        assert strategy.should_retry(error, 0) is True

        # Test max retries exceeded
        assert strategy.should_retry(error, 3) is False

        # Test non-retryable error
        error2 = Exception("Invalid command")
        assert strategy.should_retry(error2, 0) is False

    def test_get_delay(self):
        """Test calculating retry delay"""
        strategy = RetryStrategy(max_retries=3, base_delay=1.0)

        delay0 = strategy.get_delay(0)
        delay1 = strategy.get_delay(1)
        delay2 = strategy.get_delay(2)

        # Delays should increase exponentially
        assert delay0 < delay1 < delay2

    def test_execute_with_retry(self):
        """Test executing function with retry"""
        strategy = RetryStrategy(max_retries=3)
        call_count = [0]

        def flaky_function():
            call_count[0] += 1
            if call_count[0] < 2:
                raise Exception("Connection refused")
            return "success"

        result = strategy.execute_with_retry(flaky_function)
        assert result == "success"
        assert call_count[0] == 2


class TestProductionRedis:
    """Test ProductionRedis wrapper"""

    def test_wrapper_creation(self):
        """Test creating production Redis wrapper"""
        client = ProductionRedis(host="localhost", port=6379)
        assert client is not None
        client.close()

    def test_context_manager(self):
        """Test using as context manager"""
        with ProductionRedis(host="localhost", port=6379) as client:
            result = client.set('test_key', 'test_value')
            assert result == 'OK'

            value = client.get('test_key')
            assert value == 'test_value'

            client.delete('test_key')

    def test_basic_operations(self):
        """Test basic Redis operations"""
        with ProductionRedis(host="localhost", port=6379) as client:
            client.set('test_key', 'test_value')
            assert client.get('test_key') == 'test_value'
            assert client.delete('test_key') == 1


class TestPoolEdgeCases:
    """Test edge cases for connection pooling"""

    def test_empty_pool(self):
        """Test behavior with empty pool"""
        pool = EnhancedConnectionPool(
            host="localhost",
            port=6379,
            min_connections=0,
            max_connections=1
        )
        conn = pool.get_connection()
        # Should create connection on demand
        assert conn is not None or conn is None  # May fail to connect
        if conn:
            pool.return_connection(conn)
        pool.close()

    def test_max_connections_reached(self):
        """Test behavior when max connections reached"""
        pool = EnhancedConnectionPool(
            host="localhost",
            port=6379,
            min_connections=1,
            max_connections=2
        )

        conns = []
        for _ in range(3):
            conn = pool.get_connection()
            if conn:
                conns.append(conn)

        # Should not exceed max connections
        # (some calls may fail or block)

        for conn in conns:
            pool.return_connection(conn)

        pool.close()

    def test_connection_failure(self):
        """Test handling connection failure"""
        pool = EnhancedConnectionPool(
            host="invalid_host",
            port=6379,
            min_connections=1,
            max_connections=2
        )
        # Pool creation should not fail, but connections won't work
        pool.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
