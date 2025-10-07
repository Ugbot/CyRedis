# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Shared State Manager for CyRedis Web Application Support.
Provides shared state management for worker processes with distributed locks, counters, and shared data structures.
"""

import json
import time
import uuid
from typing import Dict, List, Any

# Import core Redis functionality
from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class SharedStateManager:
    """
    Shared state management for worker processes.
    Provides distributed locks, counters, and shared data structures.
    """

    def __cinit__(self, CyRedisClient redis_client):
        self.redis_client = redis_client
        self.locks_key = "shared:locks"
        self.counters_key = "shared:counters"
        self.data_key = "shared:data"

    def acquire_lock(self, lock_name: str, timeout: int = 30) -> str:
        """Acquire a distributed lock"""
        lock_id = str(uuid.uuid4())
        lock_key = f"{self.locks_key}:{lock_name}"

        current_time = time.time()
        expires_at = current_time + timeout

        # Try to acquire lock
        if self.redis_client.set(lock_key, lock_id, ex=timeout, nx=True):
            return lock_id

        return None

    def release_lock(self, lock_name: str, lock_id: str) -> bool:
        """Release a distributed lock"""
        lock_key = f"{self.locks_key}:{lock_name}"
        stored_id = self.redis_client.get(lock_key)

        if stored_id == lock_id:
            self.redis_client.delete(lock_key)
            return True

        return False

    def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        """Increment a shared counter"""
        counter_key = f"{self.counters_key}:{counter_name}"
        return self.redis_client.incr(counter_key, increment)

    def get_counter(self, counter_name: str) -> int:
        """Get counter value"""
        counter_key = f"{self.counters_key}:{counter_name}"
        value = self.redis_client.get(counter_key)
        return int(value) if value else 0

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        """Set shared data with optional expiry"""
        data_key = f"{self.data_key}:{key}"
        if expiry:
            self.redis_client.set(data_key, json.dumps(data), ex=expiry)
        else:
            self.redis_client.set(data_key, json.dumps(data))

    def get_shared_data(self, key: str) -> Any:
        """Get shared data"""
        data_key = f"{self.data_key}:{key}"
        data = self.redis_client.get(data_key)
        return json.loads(data) if data else None

    def publish_event(self, channel: str, event_data: Dict[str, Any]):
        """Publish event to shared channel"""
        self.redis_client.publish(channel, json.dumps(event_data))

    def subscribe_to_events(self, channels: List[str]) -> List[str]:
        """Subscribe to events (returns message format)"""
        # This is a simplified implementation
        # In production, you'd use Redis pub/sub properly
        return []
