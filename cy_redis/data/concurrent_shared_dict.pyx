# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Concurrent Shared Dictionary for CyRedis Web Application Support.
Thread-safe and process-safe dictionary that can be accessed by multiple users.
"""

import json
import time
from typing import Dict, List, Any

# Import dependencies
from cy_redis.core.cy_redis_client cimport CyRedisClient
from cy_redis.features.distributed import CyDistributedLock


cdef class ConcurrentSharedDict:
    """
    Concurrent shared dictionary for all users with Redis replication.
    Thread-safe and process-safe dictionary that can be accessed by multiple users.
    """

    def __cinit__(self, str dict_name, CyRedisClient redis_client):
        self.dict_name = dict_name
        self.redis_client = redis_client
        self.dict_key = f"shared_dict:{dict_name}"
        self.lock_key = f"{self.dict_key}:lock"
        self.local_cache = {}
        self.cache_ttl = 30  # 30 seconds cache
        self.last_sync = 0

        # Initialize distributed lock for concurrency control
        self.lock = CyDistributedLock(redis_client, self.lock_key, ttl_ms=5000)

    cdef dict _load_from_redis(self):
        """Load dictionary from Redis with caching"""
        cdef str data = self.redis_client.get(self.dict_key)
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return {}
        return {}

    cdef void _save_to_redis(self, dict data):
        """Save dictionary to Redis"""
        cdef str json_data = json.dumps(data, sort_keys=True)
        self.redis_client.set(self.dict_key, json_data)

    cdef bint _is_cache_valid(self):
        """Check if local cache is still valid"""
        return (time.time() - self.last_sync) < self.cache_ttl

    cdef void _invalidate_cache(self):
        """Invalidate local cache"""
        self.last_sync = 0
        self.local_cache.clear()

    cdef dict _ensure_synced(self):
        """Ensure local cache is synced with Redis"""
        if not self._is_cache_valid():
            self.local_cache = self._load_from_redis()
            self.last_sync = <long>time.time()
        return self.local_cache

    # Dictionary interface - thread-safe and process-safe
    def __getitem__(self, key):
        """Get item with automatic sync"""
        cdef dict data = self._ensure_synced()
        if key not in data:
            raise KeyError(key)
        return data[key]

    def __setitem__(self, key, value):
        """Set item with distributed locking"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            data = self._load_from_redis()  # Always get latest from Redis
            data[key] = value
            self._save_to_redis(data)
            self._invalidate_cache()  # Force sync on next access
        finally:
            self.lock.release()

    def __delitem__(self, key):
        """Delete item with distributed locking"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            data = self._load_from_redis()
            if key in data:
                del data[key]
                self._save_to_redis(data)
                self._invalidate_cache()
        finally:
            self.lock.release()

    def __contains__(self, key):
        """Check if key exists"""
        cdef dict data = self._ensure_synced()
        return key in data

    def __len__(self):
        """Get dictionary length"""
        cdef dict data = self._ensure_synced()
        return len(data)

    def __iter__(self):
        """Iterate over keys"""
        cdef dict data = self._ensure_synced()
        return iter(data)

    def keys(self):
        """Get dictionary keys"""
        cdef dict data = self._ensure_synced()
        return data.keys()

    def values(self):
        """Get dictionary values"""
        cdef dict data = self._ensure_synced()
        return data.values()

    def items(self):
        """Get dictionary items"""
        cdef dict data = self._ensure_synced()
        return data.items()

    def get(self, key, default=None):
        """Get with default value"""
        cdef dict data = self._ensure_synced()
        return data.get(key, default)

    # Advanced concurrent operations
    def update(self, other: Dict[str, Any]):
        """Update dictionary with another dict (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            data.update(other)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    def clear(self):
        """Clear the dictionary (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            self._save_to_redis({})
            self._invalidate_cache()
        finally:
            self.lock.release()

    def pop(self, key, default=None):
        """Pop item from dictionary (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef object result = data.pop(key, default)
            self._save_to_redis(data)
            self._invalidate_cache()
            return result
        finally:
            self.lock.release()

    def popitem(self):
        """Pop random item from dictionary (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef tuple result = data.popitem()
            self._save_to_redis(data)
            self._invalidate_cache()
            return result
        finally:
            self.lock.release()

    def setdefault(self, key, default=None):
        """Set default value if key doesn't exist (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            if key not in data:
                data[key] = default
                self._save_to_redis(data)
                self._invalidate_cache()
            return data[key]
        finally:
            self.lock.release()

    # Atomic numeric operations
    def increment(self, key: str, amount: int = 1) -> int:
        """Atomically increment a numeric value"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef int current = data.get(key, 0)
            if not isinstance(current, int):
                current = 0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self._invalidate_cache()
            return current
        finally:
            self.lock.release()

    def increment_float(self, key: str, amount: float = 1.0) -> float:
        """Atomically increment a float value"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            cdef float current = data.get(key, 0.0)
            if not isinstance(current, (int, float)):
                current = 0.0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self._invalidate_cache()
            return current
        finally:
            self.lock.release()

    # Bulk operations for efficiency
    def bulk_update(self, updates: Dict[str, Any]):
        """Bulk update multiple keys at once (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            data.update(updates)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    def bulk_get(self, keys: List[str]) -> List[Any]:
        """Bulk get multiple keys efficiently"""
        cdef dict data = self._ensure_synced()
        return [data.get(key) for key in keys]

    def multi_get(self, *keys: str) -> Dict[str, Any]:
        """Get multiple keys as a dictionary"""
        cdef dict data = self._ensure_synced()
        return {key: data.get(key) for key in keys}

    def multi_set(self, mapping: Dict[str, Any]):
        """Set multiple key-value pairs (atomic operation)"""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError(f"Could not acquire lock for shared dict '{self.dict_name}'")

        try:
            cdef dict data = self._load_from_redis()
            data.update(mapping)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    # Synchronization and statistics
    def sync(self):
        """Force synchronization with Redis"""
        self._invalidate_cache()
        self._ensure_synced()

    def is_synced(self) -> bool:
        """Check if local cache is synchronized"""
        return self._is_cache_valid()

    def get_stats(self) -> Dict[str, Any]:
        """Get dictionary statistics"""
        cdef dict data = self._ensure_synced()
        cdef long total_size = len(json.dumps(data))

        return {
            'name': self.dict_name,
            'key_count': len(data),
            'total_size_bytes': total_size,
            'cache_age_seconds': <long>time.time() - self.last_sync,
            'cache_ttl_seconds': self.cache_ttl,
            'lock_key': self.lock_key,
            'redis_key': self.dict_key
        }

    def copy(self) -> Dict[str, Any]:
        """Create a copy of the dictionary"""
        cdef dict data = self._ensure_synced()
        return data.copy()

    def __repr__(self):
        """String representation"""
        cdef dict data = self._ensure_synced()
        return f"ConcurrentSharedDict('{self.dict_name}', {len(data)} items)"

    def __str__(self):
        """String representation"""
        return self.__repr__()


# Python wrapper for ConcurrentSharedDict
class ConcurrentSharedDictWrapper:
    """
    Python wrapper for ConcurrentSharedDict providing dict-like interface.
    """

    def __init__(self, dict_name: str, redis_client: CyRedisClient):
        self.dict = ConcurrentSharedDict(dict_name, redis_client)

    def __getitem__(self, key):
        return self.dict[key]

    def __setitem__(self, key, value):
        self.dict[key] = value

    def __delitem__(self, key):
        del self.dict[key]

    def __contains__(self, key):
        return key in self.dict

    def __len__(self):
        return len(self.dict)

    def __iter__(self):
        return iter(self.dict)

    def keys(self):
        return self.dict.keys()

    def values(self):
        return self.dict.values()

    def items(self):
        return self.dict.items()

    def get(self, key, default=None):
        return self.dict.get(key, default)

    def update(self, other):
        self.dict.update(other)

    def clear(self):
        self.dict.clear()

    def pop(self, key, default=None):
        return self.dict.pop(key, default)

    def popitem(self):
        return self.dict.popitem()

    def setdefault(self, key, default=None):
        return self.dict.setdefault(key, default)

    def increment(self, key: str, amount: int = 1) -> int:
        return self.dict.increment(key, amount)

    def increment_float(self, key: str, amount: float = 1.0) -> float:
        return self.dict.increment_float(key, amount)

    def bulk_update(self, updates):
        self.dict.bulk_update(updates)

    def bulk_get(self, keys):
        return self.dict.bulk_get(keys)

    def multi_get(self, *keys):
        return self.dict.multi_get(*keys)

    def multi_set(self, mapping):
        self.dict.multi_set(mapping)

    def sync(self):
        self.dict.sync()

    def is_synced(self) -> bool:
        return self.dict.is_synced()

    def get_stats(self):
        return self.dict.get_stats()

    def copy(self):
        return self.dict.copy()

    def __repr__(self):
        return self.dict.__repr__()

    def __str__(self):
        return self.dict.__str__()
