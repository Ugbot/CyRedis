"""
Integration tests for messaging workflows.

Tests complete messaging patterns including:
- Reliable queue producer/consumer
- Worker queue patterns
- Message retries and dead letter queues
- Consumer groups
- Message acknowledgment
- Priority queues
"""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor


try:
    from redis_wrapper import ReliableQueue, WorkerQueue
    from messaging import ReliableQueue as PyReliableQueue
    MESSAGING_AVAILABLE = True
except ImportError:
    MESSAGING_AVAILABLE = False


@pytest.mark.integration
@pytest.mark.skipif(not MESSAGING_AVAILABLE, reason="Messaging components not available")
class TestReliableQueueFlow:
    """Test reliable queue workflows."""

    def test_simple_producer_consumer(self, hp_redis_client, unique_key):
        """Test simple producer/consumer pattern."""
        queue_name = f"{unique_key}:reliable"
        queue = ReliableQueue(hp_redis_client, queue_name)

        # Producer: push messages
        message_ids = []
        for i in range(10):
            msg_id = queue.push({"data": f"message_{i}"})
            message_ids.append(msg_id)

        assert len(message_ids) == 10

        # Consumer: pop messages
        consumed = []
        for _ in range(10):
            messages = queue.pop(count=1)
            if messages:
                consumed.extend(messages)
                # Acknowledge message
                for msg in messages:
                    queue.ack(msg['id'])

        # Should consume all messages
        assert len(consumed) == 10

        # Verify data
        for i, msg in enumerate(sorted(consumed, key=lambda m: m['data'])):
            assert msg['data'] == {"data": f"message_{i}"}

    def test_priority_queue(self, hp_redis_client, unique_key):
        """Test priority-based message ordering."""
        queue_name = f"{unique_key}:priority"
        queue = ReliableQueue(hp_redis_client, queue_name)

        # Push messages with different priorities
        queue.push({"msg": "low"}, priority=10)
        queue.push({"msg": "high"}, priority=1)
        queue.push({"msg": "medium"}, priority=5)

        # Pop messages - should come out in priority order
        messages = queue.pop(count=3)

        assert len(messages) == 3
        assert messages[0]['data']['msg'] == "high"
        assert messages[1]['data']['msg'] == "medium"
        assert messages[2]['data']['msg'] == "low"

        # Cleanup
        for msg in messages:
            queue.ack(msg['id'])

    def test_visibility_timeout(self, hp_redis_client, unique_key):
        """Test message visibility timeout."""
        queue_name = f"{unique_key}:visibility"
        queue = ReliableQueue(hp_redis_client, queue_name, visibility_timeout=2)

        # Push message
        msg_id = queue.push({"data": "test"})

        # Pop message
        messages = queue.pop(count=1)
        assert len(messages) == 1

        # Message should not be available immediately
        messages2 = queue.pop(count=1)
        assert len(messages2) == 0

        # Wait for visibility timeout
        time.sleep(2.5)

        # Message should be available again
        messages3 = queue.pop(count=1)
        assert len(messages3) == 1

        # Cleanup
        queue.ack(messages3[0]['id'])

    def test_retry_mechanism(self, hp_redis_client, unique_key):
        """Test message retry mechanism."""
        queue_name = f"{unique_key}:retry"
        queue = ReliableQueue(
            hp_redis_client,
            queue_name,
            max_retries=3,
            visibility_timeout=1
        )

        # Push message
        msg_id = queue.push({"data": "retry_test"})

        # Pop and fail multiple times
        for i in range(3):
            messages = queue.pop(count=1)
            assert len(messages) == 1

            # Simulate processing failure (nack)
            queue.nack(messages[0]['id'])

            # Wait for visibility timeout
            time.sleep(1.5)

        # After max retries, should go to dead letter queue
        time.sleep(1.5)
        messages = queue.pop(count=1)
        assert len(messages) == 0  # Should be in DLQ

    def test_concurrent_consumers(self, hp_redis_client, unique_key):
        """Test multiple concurrent consumers."""
        queue_name = f"{unique_key}:concurrent"
        queue = ReliableQueue(hp_redis_client, queue_name)

        # Push 100 messages
        for i in range(100):
            queue.push({"data": f"message_{i}"})

        consumed = []
        lock = threading.Lock()

        def consumer_worker():
            while True:
                messages = queue.pop(count=5)
                if not messages:
                    break

                with lock:
                    consumed.extend(messages)

                # Acknowledge messages
                for msg in messages:
                    queue.ack(msg['id'])

                time.sleep(0.01)  # Simulate processing

        # Run 5 concurrent consumers
        threads = []
        for _ in range(5):
            t = threading.Thread(target=consumer_worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=10)

        # Should consume all messages
        assert len(consumed) == 100


@pytest.mark.integration
@pytest.mark.skipif(not MESSAGING_AVAILABLE, reason="Messaging components not available")
class TestWorkerQueueFlow:
    """Test worker queue workflows."""

    def test_task_distribution(self, hp_redis_client, unique_key):
        """Test task distribution to workers."""
        queue_name = f"{unique_key}:worker"
        queue = WorkerQueue(hp_redis_client, queue_name)

        results = []
        lock = threading.Lock()

        def worker_function(task_data):
            """Worker that processes tasks."""
            result = task_data['value'] * 2
            with lock:
                results.append(result)
            return result

        # Start workers
        def run_worker():
            while True:
                task = queue.get_task(timeout=2)
                if task is None:
                    break

                try:
                    result = worker_function(task['data'])
                    queue.complete_task(task['id'], result)
                except Exception as e:
                    queue.fail_task(task['id'], str(e))

        # Submit tasks
        for i in range(20):
            queue.submit_task({"value": i})

        # Run workers
        threads = []
        for _ in range(3):
            t = threading.Thread(target=run_worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=10)

        # Should process all tasks
        assert len(results) == 20

        # Verify results
        expected = sorted([i * 2 for i in range(20)])
        assert sorted(results) == expected

    def test_task_failure_handling(self, hp_redis_client, unique_key):
        """Test task failure handling."""
        queue_name = f"{unique_key}:worker_fail"
        queue = WorkerQueue(hp_redis_client, queue_name, max_retries=2)

        attempt_count = {}
        lock = threading.Lock()

        def failing_worker(task_data):
            """Worker that fails initially."""
            task_id = task_data['id']

            with lock:
                attempt_count[task_id] = attempt_count.get(task_id, 0) + 1

            # Fail first 2 attempts
            if attempt_count[task_id] < 3:
                raise Exception("Simulated failure")

            return "success"

        # Submit task
        task_id = queue.submit_task({"id": "task_1"})

        # Process with retries
        for _ in range(4):
            task = queue.get_task(timeout=2)
            if task is None:
                break

            try:
                result = failing_worker(task['data'])
                queue.complete_task(task['id'], result)
            except Exception as e:
                queue.fail_task(task['id'], str(e))

            time.sleep(0.5)

        # Should have attempted 3 times
        assert attempt_count.get("task_1", 0) == 3


@pytest.mark.integration
class TestStreamConsumerGroups:
    """Test Redis Streams consumer groups."""

    def test_consumer_group_basic(self, redis_client, unique_key):
        """Test basic consumer group functionality."""
        stream = f"{unique_key}:stream"
        group = "test_group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"

        try:
            # Create consumer group
            redis_client.xgroup_create(stream, group, id="0", mkstream=True)

            # Add messages
            for i in range(10):
                redis_client.xadd(stream, {"data": f"message_{i}"})

            # Consumer 1 reads
            messages1 = redis_client.xreadgroup(
                group, consumer1, {stream: ">"}, count=5
            )

            # Consumer 2 reads
            messages2 = redis_client.xreadgroup(
                group, consumer2, {stream: ">"}, count=5
            )

            # Each should get 5 messages
            assert len(messages1[0][1]) == 5
            assert len(messages2[0][1]) == 5

            # Acknowledge messages
            for msg_id, _ in messages1[0][1]:
                redis_client.xack(stream, group, msg_id)

            for msg_id, _ in messages2[0][1]:
                redis_client.xack(stream, group, msg_id)

        finally:
            # Cleanup
            try:
                redis_client.xgroup_destroy(stream, group)
            except Exception:
                pass
            redis_client.delete(stream)

    def test_consumer_group_pending(self, redis_client, unique_key):
        """Test pending messages in consumer group."""
        stream = f"{unique_key}:stream"
        group = "test_group"
        consumer = "consumer1"

        try:
            # Create group and add messages
            redis_client.xgroup_create(stream, group, id="0", mkstream=True)

            for i in range(5):
                redis_client.xadd(stream, {"data": f"message_{i}"})

            # Read without acknowledging
            messages = redis_client.xreadgroup(
                group, consumer, {stream: ">"}, count=5
            )

            assert len(messages[0][1]) == 5

            # Check pending messages
            pending = redis_client.xpending(stream, group)
            assert pending['pending'] == 5

            # Acknowledge all
            for msg_id, _ in messages[0][1]:
                redis_client.xack(stream, group, msg_id)

            # Pending should be 0
            pending = redis_client.xpending(stream, group)
            assert pending['pending'] == 0

        finally:
            try:
                redis_client.xgroup_destroy(stream, group)
            except Exception:
                pass
            redis_client.delete(stream)


@pytest.mark.integration
class TestPubSubFlow:
    """Test pub/sub messaging flows."""

    def test_pubsub_single_channel(self, redis_client, unique_key):
        """Test pub/sub on single channel."""
        channel = f"{unique_key}:channel"
        messages_received = []

        def subscriber():
            pubsub = redis_client.pubsub()
            pubsub.subscribe(channel)

            for message in pubsub.listen():
                if message['type'] == 'message':
                    messages_received.append(message['data'])
                    if len(messages_received) == 5:
                        break

            pubsub.unsubscribe(channel)
            pubsub.close()

        # Start subscriber
        sub_thread = threading.Thread(target=subscriber)
        sub_thread.start()

        # Wait for subscription
        time.sleep(0.5)

        # Publish messages
        for i in range(5):
            redis_client.publish(channel, f"message_{i}")
            time.sleep(0.1)

        # Wait for subscriber
        sub_thread.join(timeout=5)

        # Should receive all messages
        assert len(messages_received) == 5
        assert messages_received == [f"message_{i}" for i in range(5)]

    def test_pubsub_pattern(self, redis_client, unique_key):
        """Test pub/sub with pattern matching."""
        pattern = f"{unique_key}:*"
        messages_received = []

        def subscriber():
            pubsub = redis_client.pubsub()
            pubsub.psubscribe(pattern)

            for message in pubsub.listen():
                if message['type'] == 'pmessage':
                    messages_received.append({
                        'channel': message['channel'],
                        'data': message['data']
                    })
                    if len(messages_received) == 3:
                        break

            pubsub.punsubscribe(pattern)
            pubsub.close()

        # Start subscriber
        sub_thread = threading.Thread(target=subscriber)
        sub_thread.start()

        # Wait for subscription
        time.sleep(0.5)

        # Publish to different channels
        redis_client.publish(f"{unique_key}:ch1", "message1")
        redis_client.publish(f"{unique_key}:ch2", "message2")
        redis_client.publish(f"{unique_key}:ch3", "message3")

        # Wait for subscriber
        sub_thread.join(timeout=5)

        # Should receive all messages
        assert len(messages_received) == 3

    def test_pubsub_multiple_subscribers(self, redis_client, unique_key):
        """Test multiple subscribers on same channel."""
        channel = f"{unique_key}:multi"
        subscriber1_msgs = []
        subscriber2_msgs = []

        def subscriber(msg_list):
            pubsub = redis_client.pubsub()
            pubsub.subscribe(channel)

            for message in pubsub.listen():
                if message['type'] == 'message':
                    msg_list.append(message['data'])
                    if len(msg_list) == 3:
                        break

            pubsub.unsubscribe(channel)
            pubsub.close()

        # Start subscribers
        thread1 = threading.Thread(target=subscriber, args=(subscriber1_msgs,))
        thread2 = threading.Thread(target=subscriber, args=(subscriber2_msgs,))

        thread1.start()
        thread2.start()

        # Wait for subscriptions
        time.sleep(0.5)

        # Publish messages
        for i in range(3):
            redis_client.publish(channel, f"message_{i}")
            time.sleep(0.1)

        # Wait for subscribers
        thread1.join(timeout=5)
        thread2.join(timeout=5)

        # Both should receive all messages
        assert len(subscriber1_msgs) == 3
        assert len(subscriber2_msgs) == 3
        assert subscriber1_msgs == subscriber2_msgs
