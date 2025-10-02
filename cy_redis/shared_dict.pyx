# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
Shared Dictionary - Replicated dictionary via Redis with concurrency control
"""

import json
import time
import uuid
from typing import Dict, Any, Optional, Union, Iterator
from concurrent.futures import ThreadPoolExecutor

# Import our optimized components
from cy_redis.cy_redis_client import CyRedisClient
from cy_redis.distributed import CyDistributedLock

# Shared dictionary with Redis replication and concurrency control
cdef class CySharedDict:
    """
    High-performance shared dictionary replicated via Redis with concurrency control.
    """

    cdef CyRedisClient redis
    cdef str dict_key
    cdef str lock_key
    cdef CyDistributedLock lock
    cdef bint use_compression
    cdef ThreadPoolExecutor executor
    cdef dict local_cache
    cdef long cache_ttl
    cdef long last_sync

    def __cinit__(self, CyRedisClient redis_client, str dict_key,
                  bint use_compression=True, long cache_ttl=30):
        self.redis = redis_client
        self.dict_key = dict_key
        self.lock_key = f"{dict_key}:lock"
        self.lock = CyDistributedLock(redis_client, self.lock_key, ttl_ms=5000)
        self.use_compression = use_compression
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.local_cache = {}
        self.cache_ttl = cache_ttl
        self.last_sync = 0

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cdef dict _load_from_redis(self):
        """Load dictionary from Redis."""
        cdef str data = self.redis.get(self.dict_key)
        if data:
            try:
                if self.use_compression and data.startswith("COMPRESSED:"):
                    # Decompress if needed
                    import zlib
                    cdef str hex_data = data[11:]
                    cdef bytes compressed = bytes.fromhex(hex_data)
                    cdef bytes decompressed = zlib.decompress(compressed)
                    data = decompressed.decode('utf-8')

                return json.loads(data)
            except (json.JSONDecodeError, zlib.error):
                return {}
        return {}

    cdef void _save_to_redis(self, dict data):
        """Save dictionary to Redis."""
        cdef str json_data = json.dumps(data, sort_keys=True)

        if self.use_compression and len(json_data) > 1024:
            # Compress large data
            import zlib
            cdef bytes compressed = zlib.compress(json_data.encode('utf-8'))
            json_data = f"COMPRESSED:{compressed.hex()}"

        self.redis.set(self.dict_key, json_data)

    cdef bint _is_cache_valid(self):
        """Check if local cache is still valid."""
        return (time.time() - self.last_sync) < self.cache_ttl

    cdef void _invalidate_cache(self):
        """Invalidate local cache."""
        self.last_sync = 0
        self.local_cache.clear()

    cdef dict _ensure_synced(self):
        """Ensure local cache is synced with Redis."""
        if not self._is_cache_valid():
            self.local_cache = self._load_from_redis()
            self.last_sync = <long>time.time()
        return self.local_cache

    # Dictionary interface implementation
    def __getitem__(self, key):
        """Get item from shared dictionary."""
        cdef dict data = self._ensure_synced()
        return data[key]

    def __setitem__(self, key, value):
        """Set item in shared dictionary with concurrency control."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()  # Always get latest
            data[key] = value
            self._save_to_redis(data)
            self._invalidate_cache()  # Force sync on next access
        finally:
            self.lock.release()

    def __delitem__(self, key):
        """Delete item from shared dictionary with concurrency control."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            del data[key]
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    def __contains__(self, key):
        """Check if key exists."""
        cdef dict data = self._ensure_synced()
        return key in data

    def __len__(self):
        """Get dictionary length."""
        cdef dict data = self._ensure_synced()
        return len(data)

    def __iter__(self):
        """Iterate over keys."""
        cdef dict data = self._ensure_synced()
        return iter(data)

    def keys(self):
        """Get dictionary keys."""
        cdef dict data = self._ensure_synced()
        return data.keys()

    def values(self):
        """Get dictionary values."""
        cdef dict data = self._ensure_synced()
        return data.values()

    def items(self):
        """Get dictionary items."""
        cdef dict data = self._ensure_synced()
        return data.items()

    def get(self, key, default=None):
        """Get with default value."""
        cdef dict data = self._ensure_synced()
        return data.get(key, default)

    # Advanced operations with concurrency control
    cpdef void update(self, dict other):
        """Update dictionary with another dict."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            data.update(other)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    cpdef void clear(self):
        """Clear the dictionary."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            self._save_to_redis({})
            self._invalidate_cache()
        finally:
            self.lock.release()

    cpdef object pop(self, key, default=None):
        """Pop item from dictionary."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            cdef object result = data.pop(key, default)
            self._save_to_redis(data)
            self._invalidate_cache()
            return result
        finally:
            self.lock.release()

    cpdef tuple popitem(self):
        """Pop random item from dictionary."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            cdef tuple result = data.popitem()
            self._save_to_redis(data)
            self._invalidate_cache()
            return result
        finally:
            self.lock.release()

    cpdef void setdefault(self, key, default=None):
        """Set default value if key doesn't exist."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            data.setdefault(key, default)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    # Bulk operations
    cpdef void bulk_update(self, dict updates):
        """Bulk update multiple keys at once."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            data.update(updates)
            self._save_to_redis(data)
            self._invalidate_cache()
        finally:
            self.lock.release()

    cpdef list bulk_get(self, list keys):
        """Bulk get multiple keys."""
        cdef dict data = self._ensure_synced()
        return [data.get(key) for key in keys]

    # Atomic operations
    cpdef long increment(self, str key, long amount=1):
        """Atomically increment a numeric value."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            cdef long current = data.get(key, 0)
            if not isinstance(current, int):
                current = 0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self._invalidate_cache()
            return current
        finally:
            self.lock.release()

    cpdef double increment_float(self, str key, double amount=1.0):
        """Atomically increment a float value."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            cdef dict data = self._load_from_redis()
            cdef double current = data.get(key, 0.0)
            if not isinstance(current, (int, float)):
                current = 0.0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self._invalidate_cache()
            return current
        finally:
            self.lock.release()

    # Synchronization methods
    cpdef void sync(self):
        """Force synchronization with Redis."""
        self._invalidate_cache()
        self._ensure_synced()

    cpdef bint is_synced(self):
        """Check if local cache is synchronized."""
        return self._is_cache_valid()

    cpdef dict get_stats(self):
        """Get dictionary statistics."""
        cdef dict data = self._ensure_synced()
        cdef long total_size = len(json.dumps(data))
        cdef bint compressed = self.use_compression and total_size > 1024

        return {
            'key_count': len(data),
            'total_size_bytes': total_size,
            'compression_enabled': self.use_compression,
            'is_compressed': compressed,
            'cache_age_seconds': <long>time.time() - self.last_sync,
            'cache_ttl_seconds': self.cache_ttl,
            'lock_key': self.lock_key
        }

    # Copy operations
    def copy(self):
        """Create a copy of the dictionary."""
        cdef dict data = self._ensure_synced()
        return data.copy()

    def __repr__(self):
        """String representation."""
        cdef dict data = self._ensure_synced()
        return f"SharedDict({dict(data)})"


# Shared dictionary manager for multiple dictionaries
cdef class CySharedDictManager:
    """
    Manager for multiple shared dictionaries with connection pooling.
    """

    cdef CyRedisClient redis
    cdef dict dicts
    cdef ThreadPoolExecutor executor

    def __cinit__(self, CyRedisClient redis_client):
        self.redis = redis_client
        self.dicts = {}
        self.executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef CySharedDict get_dict(self, str name, bint use_compression=True, long cache_ttl=30):
        """Get or create a shared dictionary."""
        if name not in self.dicts:
            self.dicts[name] = CySharedDict(self.redis, f"shared_dict:{name}",
                                           use_compression, cache_ttl)
        return self.dicts[name]

    cpdef void delete_dict(self, str name):
        """Delete a shared dictionary."""
        if name in self.dicts:
            # Clear Redis data
            cdef CySharedDict d = self.dicts[name]
            d.clear()
            del self.dicts[name]

    cpdef list list_dicts(self):
        """List all managed dictionaries."""
        return list(self.dicts.keys())

    cpdef dict get_global_stats(self):
        """Get statistics for all managed dictionaries."""
        cdef dict stats = {}
        for name, d in self.dicts.items():
            stats[name] = d.get_stats()
        return stats
