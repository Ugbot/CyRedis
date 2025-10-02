# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
High-Performance Cython Messaging Components
Optimized implementations of queues, streams, pubsub, and messaging patterns.
"""

import json
import time
import uuid
from typing import Dict, List, Optional, Any, Callable, Union
from concurrent.futures import ThreadPoolExecutor

# Import our optimized Redis client
from cy_redis.cy_redis_client import CyRedisClient


# Reliable Queue with C-level optimization
cdef class CyReliableQueue:
    """
    High-performance reliable queue with visibility timeouts, retries, and dead letter queues.
    """

    def __cinit__(self, redis_client, str queue_name,
                  int visibility_timeout=30, int max_retries=3,
                  str dead_letter_queue=None):
        self.redis = redis_client
        self.queue_name = queue_name
        self.visibility_timeout = visibility_timeout
        self.max_retries = max_retries
        self.dead_letter_queue = dead_letter_queue or f"{queue_name}:dead"

        # Queue keys
        self.pending_key = f"{queue_name}:pending"
        self.processing_key = f"{queue_name}:processing"
        self.failed_key = f"{queue_name}:failed"
        self.dead_key = self.dead_letter_queue

        self.executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    def push(self, message, int priority=0, int delay=0):
        """
        Push a message to the queue.

        Args:
            message: Message to enqueue
            priority: Message priority (lower = higher priority)
            delay: Delay before message becomes visible (seconds)

        Returns:
            Message ID
        """
        message_id = str(uuid.uuid4())

        message_data = {
            'id': message_id,
            'data': message,
            'timestamp': time.time(),
            'priority': priority,
            'retries': 0,
            'delay_until': time.time() + delay if delay > 0 else 0
        }

        score = message_data['delay_until'] if delay > 0 else time.time()
        if priority != 0:
            score = score + (priority * 0.000001)  # Small adjustment for priority

        try:
            # Store message data
            self.redis.set(f"{self.pending_key}:{message_id}", json.dumps(message_data))

            # Add to sorted set with score
            self.redis.execute_command(['ZADD', self.pending_key, str(score), message_id])

            return message_id
        except Exception as e:
            print(f"Error pushing message: {e}")
            return None

    def pop(self, int count=1):
        """
        Pop messages from the queue with visibility timeout.

        Args:
            count: Number of messages to pop

        Returns:
            List of (message_id, message_data) tuples
        """
        cdef list messages = []
        cdef double current_time = time.time()

        try:
            # Get messages that are ready (score <= current time)
            ready_messages = self.redis.execute_command(['ZRANGEBYSCORE', self.pending_key, '-inf', str(current_time), 'LIMIT', '0', str(count)])

            if not ready_messages:
                return messages

            for message_id in ready_messages:
                # Get message data
                message_json = self.redis.get(f"{self.pending_key}:{message_id}")
                if not message_json:
                    continue

                message_data = json.loads(message_json)

                # Move to processing queue with visibility timeout
                processing_until = current_time + self.visibility_timeout
                processing_data = message_data.copy()
                processing_data['processing_until'] = processing_until

                self.redis.set(f"{self.processing_key}:{message_id}", json.dumps(processing_data))
                self.redis.execute_command(['ZADD', self.processing_key, str(processing_until), message_id])

                # Remove from pending
                self.redis.execute_command(['ZREM', self.pending_key, message_id])
                self.redis.delete(f"{self.pending_key}:{message_id}")

                messages.append((message_id, message_data['data']))

            return messages
        except Exception as e:
            print(f"Error popping messages: {e}")
            return messages

    def ack(self, str message_id):
        """
        Acknowledge successful processing of a message.

        Args:
            message_id: ID of the message to acknowledge
        """
        try:
            # Remove from processing queue
            self.redis.execute_command(['ZREM', self.processing_key, message_id])
            self.redis.delete(f"{self.processing_key}:{message_id}")
            return True
        except Exception as e:
            print(f"Error acknowledging message {message_id}: {e}")
            return False

    def nack(self, str message_id, bint retry=True):
        """
        Negative acknowledge - message processing failed.

        Args:
            message_id: ID of the failed message
            retry: Whether to retry the message
        """
        try:
            # Get message from processing queue
            message_json = self.redis.get(f"{self.processing_key}:{message_id}")
            if not message_json:
                return False

            message_data = json.loads(message_json)

            if retry and message_data.get('retries', 0) < self.max_retries:
                # Increment retry count and move back to pending
                message_data['retries'] = message_data.get('retries', 0) + 1
                message_data['last_error'] = time.time()

                self.redis.set(f"{self.pending_key}:{message_id}", json.dumps(message_data))
                self.redis.execute_command(['ZADD', self.pending_key, str(time.time()), message_id])
            else:
                # Move to failed/dead letter queue
                message_data['failed_at'] = time.time()
                self.redis.set(f"{self.dead_key}:{message_id}", json.dumps(message_data))
                self.redis.execute_command(['ZADD', self.failed_key, str(time.time()), message_id])

            # Remove from processing
            self.redis.execute_command(['ZREM', self.processing_key, message_id])
            self.redis.delete(f"{self.processing_key}:{message_id}")

            return True
        except Exception as e:
            print(f"Error nacking message {message_id}: {e}")
            return False

    def get_stats(self):
        """Get queue statistics"""
        try:
            pending_count = self.redis.execute_command(['ZCARD', self.pending_key])
            processing_count = self.redis.execute_command(['ZCOUNT', self.processing_key, '0', str(time.time() + 86400)])  # Next 24h
            failed_count = self.redis.execute_command(['ZCARD', self.failed_key])
            dead_count = self.redis.execute_command(['ZCARD', self.dead_key])

            return {
                'pending': pending_count or 0,
                'processing': processing_count or 0,
                'failed': failed_count or 0,
                'dead': dead_count or 0,
                'total': (pending_count or 0) + (processing_count or 0) + (failed_count or 0) + (dead_count or 0)
            }
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {'pending': 0, 'processing': 0, 'failed': 0, 'dead': 0, 'total': 0}

    # Async versions
    async def push_async(self, message, int priority=0, int delay=0):
        """Async push message"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.push, message, priority, delay)

    async def pop_async(self, int count=1):
        """Async pop messages"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.pop, count)

    async def ack_async(self, str message_id):
        """Async acknowledge"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.ack, message_id)

    async def nack_async(self, str message_id, bint retry=True):
        """Async negative acknowledge"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.nack, message_id, retry)


# Python wrapper class
class ReliableQueue:
    """
    Python wrapper for CyReliableQueue with sync/async support
    """

    def __init__(self, redis_client, queue_name: str, visibility_timeout: int = 30,
                 max_retries: int = 3, dead_letter_queue: Optional[str] = None):
        self._impl = CyReliableQueue(redis_client._client, queue_name, visibility_timeout, max_retries, dead_letter_queue)

    def push(self, message, priority: int = 0, delay: int = 0):
        return self._impl.push(message, priority, delay)

    def pop(self, count: int = 1):
        return self._impl.pop(count)

    def ack(self, message_id: str):
        return self._impl.ack(message_id)

    def nack(self, message_id: str, retry: bool = True):
        return self._impl.nack(message_id, retry)

    def get_stats(self):
        return self._impl.get_stats()

    async def push_async(self, message, priority: int = 0, delay: int = 0):
        return await self._impl.push_async(message, priority, delay)

    async def pop_async(self, count: int = 1):
        return await self._impl.pop_async(count)

    async def ack_async(self, message_id: str):
        return await self._impl.ack_async(message_id)

    async def nack_async(self, message_id: str, retry: bool = True):
        return await self._impl.nack_async(message_id, retry)


# Additional messaging classes will be implemented...
# CyPubSubHub, CyStreamConsumerGroup, CyWorkerQueue, CyAMQPRouter

# Import asyncio for async methods
import asyncio