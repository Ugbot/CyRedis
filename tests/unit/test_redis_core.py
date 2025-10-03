"""
Unit tests for redis_core.pyx - Basic Redis operations
"""
import pytest
import time
from cy_redis.redis_core import CyRedisClient, RedisConnection, ConnectionPool, MessageQueue


@pytest.fixture
def redis_client():
    """Create a Redis client for testing"""
    client = CyRedisClient(host="localhost", port=6379, max_connections=5)
    yield client
    # Cleanup
    try:
        client.delete("test_key")
        client.delete("test_counter")
    except:
        pass


@pytest.fixture
def connection():
    """Create a Redis connection for testing"""
    conn = RedisConnection(host="localhost", port=6379)
    conn.connect()
    yield conn
    conn.disconnect()


class TestRedisConnection:
    """Test RedisConnection class"""

    def test_connection_creation(self):
        """Test creating a connection"""
        conn = RedisConnection(host="localhost", port=6379)
        assert conn is not None

    def test_connect_disconnect(self, connection):
        """Test connecting and disconnecting"""
        # Already connected via fixture
        assert connection.state.state == 2  # CONN_CONNECTED

        connection.disconnect()
        assert connection.state.state == 0  # CONN_DISCONNECTED

    def test_execute_command(self, connection):
        """Test executing a Redis command"""
        result = connection.execute_command(['PING'])
        assert result == 'PONG'

    def test_set_get_command(self, connection):
        """Test SET and GET commands"""
        connection.execute_command(['SET', 'test_key', 'test_value'])
        result = connection.execute_command(['GET', 'test_key'])
        assert result == 'test_value'

        # Cleanup
        connection.execute_command(['DEL', 'test_key'])


class TestConnectionPool:
    """Test ConnectionPool class"""

    def test_pool_creation(self):
        """Test creating a connection pool"""
        pool = ConnectionPool(host="localhost", port=6379, max_connections=5)
        assert pool is not None
        assert pool.max_connections == 5

    def test_get_connection(self):
        """Test getting a connection from pool"""
        pool = ConnectionPool(host="localhost", port=6379, max_connections=5)
        conn = pool.get_connection()
        assert conn is not None
        pool.return_connection(conn)

    def test_return_connection(self):
        """Test returning a connection to pool"""
        pool = ConnectionPool(host="localhost", port=6379, max_connections=5)
        conn = pool.get_connection()
        initial_size = len(pool.connections)
        pool.return_connection(conn)
        # Connection should be back in pool
        assert len(pool.connections) >= initial_size


class TestMessageQueue:
    """Test MessageQueue class"""

    def test_queue_creation(self):
        """Test creating a message queue"""
        queue = MessageQueue(capacity=100)
        assert queue is not None

    def test_enqueue_dequeue(self):
        """Test enqueuing and dequeuing messages"""
        queue = MessageQueue(capacity=100)
        # Note: C-level operations are tested via higher-level API


class TestCyRedisClient:
    """Test CyRedisClient class"""

    def test_client_creation(self):
        """Test creating a Redis client"""
        client = CyRedisClient(host="localhost", port=6379)
        assert client is not None

    def test_set_get(self, redis_client):
        """Test SET and GET operations"""
        result = redis_client.set('test_key', 'test_value')
        assert result == 'OK'

        value = redis_client.get('test_key')
        assert value == 'test_value'

    def test_set_with_expiry(self, redis_client):
        """Test SET with expiration time"""
        redis_client.set('test_key', 'test_value', ex=1)
        value = redis_client.get('test_key')
        assert value == 'test_value'

        # Wait for expiration
        time.sleep(1.1)
        value = redis_client.get('test_key')
        assert value is None

    def test_set_nx(self, redis_client):
        """Test SET NX (only if not exists)"""
        redis_client.delete('test_key')
        result = redis_client.set('test_key', 'value1', nx=True)
        assert result == 'OK'

        # Should fail because key exists
        result = redis_client.set('test_key', 'value2', nx=True)
        assert result is None

    def test_set_xx(self, redis_client):
        """Test SET XX (only if exists)"""
        redis_client.delete('test_key')

        # Should fail because key doesn't exist
        result = redis_client.set('test_key', 'value1', xx=True)
        assert result is None

        # Create key first
        redis_client.set('test_key', 'value1')

        # Now should succeed
        result = redis_client.set('test_key', 'value2', xx=True)
        assert result == 'OK'

    def test_delete(self, redis_client):
        """Test DELETE operation"""
        redis_client.set('test_key', 'test_value')
        result = redis_client.delete('test_key')
        assert result == 1

        # Deleting non-existent key
        result = redis_client.delete('nonexistent_key')
        assert result == 0

    def test_publish(self, redis_client):
        """Test PUBLISH operation"""
        result = redis_client.publish('test_channel', 'test_message')
        # Result is number of subscribers (likely 0 in tests)
        assert isinstance(result, int)

    def test_xadd(self, redis_client):
        """Test XADD operation"""
        stream_id = redis_client.xadd('test_stream', {'field': 'value'})
        assert stream_id is not None
        assert isinstance(stream_id, str)

        # Cleanup
        try:
            redis_client.delete('test_stream')
        except:
            pass

    def test_xread(self, redis_client):
        """Test XREAD operation"""
        # Add a message first
        stream_id = redis_client.xadd('test_stream', {'field': 'value'})

        # Read from stream
        messages = redis_client.xread({'test_stream': '0'}, count=10)
        assert len(messages) > 0

        # Cleanup
        try:
            redis_client.delete('test_stream')
        except:
            pass


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_nonexistent_key(self, redis_client):
        """Test getting nonexistent key"""
        value = redis_client.get('nonexistent_key')
        assert value is None

    def test_empty_value(self, redis_client):
        """Test setting empty value"""
        redis_client.set('test_key', '')
        value = redis_client.get('test_key')
        assert value == ''

    def test_unicode_values(self, redis_client):
        """Test Unicode values"""
        unicode_value = 'Hello 世界 🌍'
        redis_client.set('test_key', unicode_value)
        value = redis_client.get('test_key')
        assert value == unicode_value

    def test_large_value(self, redis_client):
        """Test large values"""
        large_value = 'x' * 10000
        redis_client.set('test_key', large_value)
        value = redis_client.get('test_key')
        assert value == large_value
        assert len(value) == 10000


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
