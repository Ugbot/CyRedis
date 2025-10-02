"""
Thin Python wrapper for Cython messaging components.
Provides sync and async interfaces over optimized Cython implementations.
"""

import asyncio
from typing import Dict, List, Optional, Any, Callable, Union
from concurrent.futures import ThreadPoolExecutor

# Import optimized Cython implementations
try:
    from cy_redis.messaging import CyReliableQueue
    _OPTIMIZED_AVAILABLE = True
except ImportError:
    _OPTIMIZED_AVAILABLE = False


class ReliableQueue:
    """
    Thin Python wrapper for CyReliableQueue.
    Provides sync and async interfaces with automatic fallback.
    """

    def __init__(self, redis_client, queue_name: str, visibility_timeout: int = 30,
                 max_retries: int = 3, dead_letter_queue: Optional[str] = None):
        if _OPTIMIZED_AVAILABLE:
            # redis_client is HighPerformanceRedis which has a client attribute
            self._impl = CyReliableQueue(redis_client.client, queue_name, visibility_timeout, max_retries, dead_letter_queue)
            self._executor = ThreadPoolExecutor(max_workers=4)
        else:
            raise RuntimeError("Optimized messaging not available")

    def push(self, message, priority: int = 0, delay: int = 0):
        """Sync push message"""
        return self._impl.push(message, priority, delay)

    def pop(self, count: int = 1):
        """Sync pop messages"""
        return self._impl.pop(count)

    def ack(self, message_id: str):
        """Sync acknowledge message"""
        return self._impl.ack(message_id)

    def nack(self, message_id: str, retry: bool = True):
        """Sync negative acknowledge message"""
        return self._impl.nack(message_id, retry)

    def get_stats(self):
        """Get queue statistics"""
        return self._impl.get_stats()

    async def push_async(self, message, priority: int = 0, delay: int = 0):
        """Async push message"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.push, message, priority, delay)

    async def pop_async(self, count: int = 1):
        """Async pop messages"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.pop, count)

    async def ack_async(self, message_id: str):
        """Async acknowledge message"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.ack, message_id)

    async def nack_async(self, message_id: str, retry: bool = True):
        """Async negative acknowledge message"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.nack, message_id, retry)


# Additional messaging components will be added here...
# PubSubHub, StreamConsumerGroup, WorkerQueue, AMQPRouter
