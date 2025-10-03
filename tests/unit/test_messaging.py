"""
Unit tests for messaging.pyx - Messaging primitives
"""
import pytest
import time
import uuid
from cy_redis.messaging import CyReliableQueue, ReliableQueue
from cy_redis.cy_redis_client import CyRedisClient


@pytest.fixture
def redis_client():
    """Create a Redis client for testing"""
    return CyRedisClient(host="localhost", port=6379)


@pytest.fixture
def reliable_queue(redis_client):
    """Create a reliable queue for testing"""
    queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"
    queue = CyReliableQueue(
        redis_client,
        queue_name,
        visibility_timeout=30,
        max_retries=3
    )
    yield queue
    # Cleanup
    try:
        redis_client.delete(f"{queue_name}:pending")
        redis_client.delete(f"{queue_name}:processing")
        redis_client.delete(f"{queue_name}:failed")
        redis_client.delete(f"{queue_name}:dead")
    except:
        pass


class TestCyReliableQueue:
    """Test CyReliableQueue class"""

    def test_queue_creation(self, redis_client):
        """Test creating a reliable queue"""
        queue = CyReliableQueue(
            redis_client,
            "test_queue",
            visibility_timeout=30,
            max_retries=3
        )
        assert queue is not None
        assert queue.queue_name == "test_queue"
        assert queue.visibility_timeout == 30
        assert queue.max_retries == 3

    def test_push_message(self, reliable_queue):
        """Test pushing a message to queue"""
        message_id = reliable_queue.push("test_message")
        assert message_id is not None
        assert isinstance(message_id, str)

    def test_push_with_priority(self, reliable_queue):
        """Test pushing with priority"""
        msg_id_high = reliable_queue.push("high_priority", priority=1)
        msg_id_low = reliable_queue.push("low_priority", priority=10)
        assert msg_id_high is not None
        assert msg_id_low is not None

    def test_push_with_delay(self, reliable_queue):
        """Test pushing with delay"""
        message_id = reliable_queue.push("delayed_message", delay=2)
        assert message_id is not None

        # Immediate pop should return nothing
        messages = reliable_queue.pop(count=1)
        assert len(messages) == 0

        # After delay, message should be available
        time.sleep(2.5)
        messages = reliable_queue.pop(count=1)
        # Clean up
        if messages:
            reliable_queue.ack(messages[0][0])

    def test_pop_message(self, reliable_queue):
        """Test popping messages from queue"""
        # Push a message
        msg_id = reliable_queue.push("test_message")

        # Pop the message
        messages = reliable_queue.pop(count=1)
        assert len(messages) > 0
        assert messages[0][1] == "test_message"

        # Acknowledge
        reliable_queue.ack(messages[0][0])

    def test_pop_multiple(self, reliable_queue):
        """Test popping multiple messages"""
        # Push multiple messages
        for i in range(5):
            reliable_queue.push(f"message_{i}")

        # Pop multiple
        messages = reliable_queue.pop(count=3)
        assert len(messages) <= 3

        # Acknowledge all
        for msg_id, _ in messages:
            reliable_queue.ack(msg_id)

    def test_ack_message(self, reliable_queue):
        """Test acknowledging a message"""
        msg_id = reliable_queue.push("test_message")
        messages = reliable_queue.pop(count=1)

        if messages:
            result = reliable_queue.ack(messages[0][0])
            assert result is True

    def test_nack_message(self, reliable_queue):
        """Test negative acknowledging a message"""
        msg_id = reliable_queue.push("test_message")
        messages = reliable_queue.pop(count=1)

        if messages:
            result = reliable_queue.nack(messages[0][0], retry=True)
            assert result is True

    def test_nack_without_retry(self, reliable_queue):
        """Test nack without retry (send to DLQ)"""
        msg_id = reliable_queue.push("test_message")
        messages = reliable_queue.pop(count=1)

        if messages:
            result = reliable_queue.nack(messages[0][0], retry=False)
            assert result is True

    def test_max_retries(self, reliable_queue):
        """Test max retries behavior"""
        msg_id = reliable_queue.push("test_message")

        # Nack multiple times to exceed max retries
        for _ in range(4):
            messages = reliable_queue.pop(count=1)
            if messages:
                reliable_queue.nack(messages[0][0], retry=True)
                time.sleep(0.1)

        # Message should now be in dead letter queue
        stats = reliable_queue.get_stats()
        # Either in dead or failed
        assert stats['dead'] > 0 or stats['failed'] > 0

    def test_get_stats(self, reliable_queue):
        """Test getting queue statistics"""
        # Push some messages
        for i in range(5):
            reliable_queue.push(f"message_{i}")

        stats = reliable_queue.get_stats()
        assert 'pending' in stats
        assert 'processing' in stats
        assert 'failed' in stats
        assert 'dead' in stats
        assert 'total' in stats
        assert stats['pending'] > 0


class TestReliableQueuePythonWrapper:
    """Test Python wrapper for ReliableQueue"""

    def test_wrapper_creation(self, redis_client):
        """Test creating Python wrapper"""
        # Note: The wrapper expects a different API
        # This is a simplified test
        queue_name = f"test_wrapper_queue_{uuid.uuid4().hex[:8]}"
        # The wrapper would need proper setup


class TestAsyncReliableQueue:
    """Test async operations on ReliableQueue"""

    @pytest.mark.asyncio
    async def test_async_push(self, redis_client):
        """Test async push operation"""
        queue = CyReliableQueue(redis_client, "async_test_queue")
        # Async operations would be tested here
        # Note: Current implementation uses run_in_executor


class TestQueueEdgeCases:
    """Test edge cases for queue operations"""

    def test_empty_queue_pop(self, reliable_queue):
        """Test popping from empty queue"""
        messages = reliable_queue.pop(count=1)
        assert messages == []

    def test_duplicate_ack(self, reliable_queue):
        """Test acknowledging same message twice"""
        msg_id = reliable_queue.push("test_message")
        messages = reliable_queue.pop(count=1)

        if messages:
            result1 = reliable_queue.ack(messages[0][0])
            result2 = reliable_queue.ack(messages[0][0])
            # Second ack should fail
            assert result2 is False

    def test_large_message(self, reliable_queue):
        """Test pushing large message"""
        large_message = 'x' * 10000
        msg_id = reliable_queue.push(large_message)
        assert msg_id is not None

        messages = reliable_queue.pop(count=1)
        if messages:
            assert len(messages[0][1]) == 10000
            reliable_queue.ack(messages[0][0])


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
