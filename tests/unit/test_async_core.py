"""
Unit tests for async_core.pyx - Async Redis operations
"""
import pytest
import asyncio
import time
from cy_redis.async_core import (
    AsyncRedisConnection, AsyncRedisClient, AsyncMessageQueue,
    AsyncRedisWrapper
)


@pytest.fixture
async def async_client():
    """Create an async Redis client for testing"""
    client = AsyncRedisWrapper(host="localhost", port=6379)
    yield client
    # Cleanup
    del client


@pytest.fixture
def message_queue():
    """Create an async message queue for testing"""
    queue = AsyncMessageQueue(capacity=100)
    yield queue


class TestAsyncMessageQueue:
    """Test AsyncMessageQueue class"""

    def test_queue_creation(self):
        """Test creating an async message queue"""
        queue = AsyncMessageQueue(capacity=100)
        assert queue is not None
        assert queue.queue.capacity == 100


class TestAsyncRedisConnection:
    """Test AsyncRedisConnection class"""

    def test_connection_creation(self):
        """Test creating an async connection"""
        conn = AsyncRedisConnection(host="localhost", port=6379)
        assert conn is not None

    def test_connect_disconnect(self):
        """Test async connect and disconnect"""
        conn = AsyncRedisConnection(host="localhost", port=6379)
        result = conn.connect()
        assert result == 0  # Success
        conn.disconnect()


class TestAsyncRedisWrapper:
    """Test AsyncRedisWrapper class"""

    def test_wrapper_creation(self):
        """Test creating async wrapper"""
        wrapper = AsyncRedisWrapper(host="localhost", port=6379)
        assert wrapper is not None

    @pytest.mark.asyncio
    async def test_async_set_get(self, async_client):
        """Test async SET and GET operations"""
        result = await async_client.set('test_async_key', 'test_value')
        assert result is not None

        value = await async_client.get('test_async_key')
        assert value is not None

        # Cleanup
        await async_client.delete('test_async_key')

    @pytest.mark.asyncio
    async def test_async_delete(self, async_client):
        """Test async DELETE operation"""
        await async_client.set('test_async_key', 'test_value')
        result = await async_client.delete('test_async_key')
        assert result is not None


class TestAsyncRedisClient:
    """Test AsyncRedisClient class"""

    def test_client_creation(self):
        """Test creating async Redis client"""
        client = AsyncRedisClient(host="localhost", port=6379)
        assert client is not None
        assert client.max_connections == 10

    def test_start_stop_workers(self):
        """Test starting and stopping worker threads"""
        client = AsyncRedisClient(host="localhost", port=6379)
        client.start_workers()
        assert client.running is True

        client.stop_workers()
        assert client.running is False


class TestAsyncConcurrency:
    """Test concurrent async operations"""

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, async_client):
        """Test multiple concurrent async operations"""
        keys = [f'async_key_{i}' for i in range(10)]

        # Set values concurrently
        set_tasks = [
            async_client.set(key, f'value_{i}')
            for i, key in enumerate(keys)
        ]
        await asyncio.gather(*set_tasks)

        # Get values concurrently
        get_tasks = [async_client.get(key) for key in keys]
        values = await asyncio.gather(*get_tasks)

        # Verify
        assert len(values) == 10

        # Cleanup
        delete_tasks = [async_client.delete(key) for key in keys]
        await asyncio.gather(*delete_tasks)


class TestAsyncEdgeCases:
    """Test edge cases for async operations"""

    @pytest.mark.asyncio
    async def test_async_nonexistent_key(self, async_client):
        """Test async get of nonexistent key"""
        value = await async_client.get('nonexistent_async_key')
        # Value may be None or an error indicator

    @pytest.mark.asyncio
    async def test_async_timeout(self):
        """Test async operation timeout"""
        client = AsyncRedisWrapper(host="localhost", port=6379)

        # Test with reasonable timeout
        try:
            result = await asyncio.wait_for(
                client.set('test_key', 'value'),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            pytest.fail("Operation timed out")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
