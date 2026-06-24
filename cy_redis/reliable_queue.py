"""
Python-level wrappers for reliable and worker queues using CyReliableQueue.
These adapt return types to the dict-based contract used in integration tests.
"""

from cy_redis.communication.messaging import CyReliableQueue


class ReliableQueue:
    def __init__(self, redis_client, queue_name: str, visibility_timeout: int = 30, max_retries: int = 3):
        self._queue = CyReliableQueue(
            redis_client,
            queue_name,
            visibility_timeout=visibility_timeout,
            max_retries=max_retries,
        )

    def push(self, message, priority: int = 0, delay: int = 0):
        return self._queue.push(message, priority=priority, delay=delay)

    def pop(self, count: int = 1):
        items = self._queue.pop(count=count)
        # Normalize to list of dicts with id/data keys
        normalized = []
        for item in items:
            if isinstance(item, tuple) and len(item) == 2:
                msg_id, data = item
                normalized.append({"id": msg_id, "data": data})
            elif isinstance(item, dict):
                normalized.append(item)
        return normalized

    def ack(self, message_id: str):
        return self._queue.ack(message_id)

    def nack(self, message_id: str, retry: bool = True):
        return self._queue.nack(message_id, retry=retry)

    def get_stats(self):
        return self._queue.get_stats()


class WorkerQueue(ReliableQueue):
    """For test compatibility; same behavior as ReliableQueue."""
    pass



