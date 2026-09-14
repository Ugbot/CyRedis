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
from typing import Any, Dict, List

# Import core Redis functionality

from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class SharedStateManager:
    """
    Shared state management for worker processes.
    Provides distributed locks, counters, and shared data structures.
    """

    cdef object redis_client
    cdef str locks_key
    cdef str counters_key
    cdef str data_key

    def __cinit__(self, CyRedisClient redis_client):
        self.redis_client = redis_client
        self.locks_key = "shared:locks"
        self.counters_key = "shared:counters"
        self.data_key = "shared:data"

    def acquire_lock(self, lock_name: str, timeout: int = 30) -> str:
        """Acquire a distributed lock"""
        # Precondition: caller/environment errors -> raise, not assert.
        if not lock_name:
            raise ValueError("lock_name must be a non-empty string")
        if timeout <= 0:
            raise ValueError("timeout must be a positive number of seconds")

        lock_id = str(uuid.uuid4())
        lock_key = f"{self.locks_key}:{lock_name}"

        # Invariant: a freshly minted UUID4 string is always 36 chars.
        assert len(lock_id) == 36, "uuid4 string must be 36 chars"
        assert lock_key.startswith(self.locks_key), "lock key must be namespaced"

        # Try to acquire lock
        if self.redis_client.set(lock_key, lock_id, ex=timeout, nx=True):
            return lock_id

        return None

    def release_lock(self, lock_name: str, lock_id: str) -> bool:
        """Release a distributed lock"""
        if not lock_name:
            raise ValueError("lock_name must be a non-empty string")
        if not lock_id:
            raise ValueError("lock_id must be a non-empty string")

        lock_key = f"{self.locks_key}:{lock_name}"
        assert lock_key.startswith(self.locks_key), "lock key must be namespaced"

        stored_id = self.redis_client.get(lock_key)

        # Only the holder of the matching token may release the lock.
        if stored_id == lock_id:
            self.redis_client.delete(lock_key)
            return True

        return False

    def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        """Increment a shared counter"""
        if not counter_name:
            raise ValueError("counter_name must be a non-empty string")

        counter_key = f"{self.counters_key}:{counter_name}"
        assert counter_key.startswith(self.counters_key), "counter key must be namespaced"

        new_value = self.redis_client.incr(counter_key, increment)

        # Postcondition: INCR returns an integer counter value.
        assert isinstance(new_value, int), "INCR must return an integer"
        return new_value

    def get_counter(self, counter_name: str) -> int:
        """Get counter value"""
        if not counter_name:
            raise ValueError("counter_name must be a non-empty string")

        counter_key = f"{self.counters_key}:{counter_name}"
        assert counter_key.startswith(self.counters_key), "counter key must be namespaced"

        value = self.redis_client.get(counter_key)
        result = int(value) if value else 0

        # Postcondition: always return a concrete integer, never None.
        assert isinstance(result, int), "counter value must be an integer"
        return result

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        """Set shared data with optional expiry"""
        if not key:
            raise ValueError("key must be a non-empty string")
        if expiry is not None and expiry <= 0:
            raise ValueError("expiry must be a positive number of seconds")

        data_key = f"{self.data_key}:{key}"
        assert data_key.startswith(self.data_key), "data key must be namespaced"

        serialized = json.dumps(data)
        # Invariant: json.dumps yields a str we can hand straight to Redis.
        assert isinstance(serialized, str), "serialized payload must be a str"

        if expiry:
            self.redis_client.set(data_key, serialized, ex=expiry)
        else:
            self.redis_client.set(data_key, serialized)

    def get_shared_data(self, key: str) -> Any:
        """Get shared data"""
        if not key:
            raise ValueError("key must be a non-empty string")

        data_key = f"{self.data_key}:{key}"
        assert data_key.startswith(self.data_key), "data key must be namespaced"

        data = self.redis_client.get(data_key)
        return json.loads(data) if data else None

    def publish_event(self, channel: str, event_data: Dict[str, Any]):
        """Publish event to shared channel"""
        if not channel:
            raise ValueError("channel must be a non-empty string")

        payload = json.dumps(event_data)
        # Invariant: serialized event is a str payload for PUBLISH.
        assert isinstance(payload, str), "serialized event must be a str"
        self.redis_client.publish(channel, payload)

    def subscribe_to_events(self, channels: List[str]) -> List[str]:
        """Subscribe to events (returns message format)"""
        # This is a simplified implementation
        # In production, you'd use Redis pub/sub properly
        return []
