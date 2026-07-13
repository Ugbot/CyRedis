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
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterator, Optional, Union

# Import our optimized components
from cy_redis.core.cy_redis_client import CyRedisClient
from cy_redis.features.distributed import CyDistributedLock


# Shared dictionary with Redis replication and concurrency control
cdef class CySharedDict:
    """
    High-performance shared dictionary replicated via Redis with concurrency control.
    """

    cdef object redis
    cdef readonly str dict_key
    cdef str lock_key
    cdef object lock
    cdef bint use_compression
    cdef object executor
    cdef dict local_cache
    cdef long cache_ttl
    cdef long last_sync

    # Fail-safe ceiling on a single decompressed payload (256 MiB). A
    # COMPRESSED: blob inflating past this is treated as corrupt/hostile
    # rather than being allowed to exhaust memory (zip-bomb guard).
    cdef long _MAX_DECOMPRESSED_BYTES

    def __cinit__(self, object redis_client, str dict_key,
                  bint use_compression=True, long cache_ttl=30):
        # Preconditions: bad caller arguments raise, they are not asserts.
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if not dict_key:
            raise ValueError("dict_key must be a non-empty string")
        if cache_ttl < 0:
            raise ValueError("cache_ttl must be non-negative")

        self._MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024
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
        cdef str hex_data
        cdef bytes compressed
        cdef bytes decompressed
        cdef object result
        import zlib

        if data:
            try:
                if self.use_compression and data.startswith("COMPRESSED:"):
                    # Decompress, bounding the inflated size so a corrupt or
                    # hostile blob cannot exhaust memory (zip-bomb guard).
                    hex_data = data[11:]
                    compressed = bytes.fromhex(hex_data)
                    assert len(compressed) >= 0, "compressed length is non-negative"
                    decompressed = zlib.decompress(
                        compressed, 15, self._MAX_DECOMPRESSED_BYTES
                    )
                    # Postcondition: bounded decompress never exceeds the cap.
                    assert len(decompressed) <= self._MAX_DECOMPRESSED_BYTES, \
                        "decompressed payload exceeds fail-safe cap"
                    data = decompressed.decode('utf-8')

                result = json.loads(data)
                # Invariant: the persisted document is always a JSON object.
                assert isinstance(result, dict), "stored payload must be a dict"
                return result
            except (json.JSONDecodeError, zlib.error):
                return {}
        return {}

    cdef void _save_to_redis(self, dict data):
        """Save dictionary to Redis."""
        assert data is not None, "cannot persist a NULL dict"

        cdef str json_data = json.dumps(data, sort_keys=True)
        cdef bytes compressed
        cdef long serialized_length = len(json_data)
        import zlib

        # Invariant: json.dumps of a dict is a non-empty str ("{}" at minimum).
        assert serialized_length >= 2, "serialized JSON object is at least '{}'"

        if self.use_compression and serialized_length > 1024:
            # Compress large data; the inflated form must stay within the cap
            # so it can be read back by _load_from_redis.
            assert serialized_length <= self._MAX_DECOMPRESSED_BYTES, \
                "payload too large to round-trip through compression"
            compressed = zlib.compress(json_data.encode('utf-8'))
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
        # Postcondition: callers always receive a concrete dict to read from.
        assert self.local_cache is not None, "local cache must be a dict"
        assert isinstance(self.local_cache, dict), "local cache must be a dict"
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
            data = self._load_from_redis()  # Always get latest
            data[key] = value
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
        finally:
            self.lock.release()

    def __delitem__(self, key):
        """Delete item from shared dictionary with concurrency control."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            del data[key]
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
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
        cdef dict data

        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            data.update(other)
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
        finally:
            self.lock.release()

    cpdef void clear(self):
        """Clear the dictionary."""
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            self._save_to_redis({})
            self.local_cache = {}
            self.last_sync = <long>time.time()
        finally:
            self.lock.release()

    cpdef object pop(self, key, default=None):
        """Pop item from dictionary."""
        cdef dict data
        cdef object result

        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            result = data.pop(key, default)
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
            return result
        finally:
            self.lock.release()

    cpdef tuple popitem(self):
        """Pop random item from dictionary."""
        cdef dict data
        cdef tuple result

        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            result = data.popitem()
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
            return result
        finally:
            self.lock.release()

    cpdef void setdefault(self, key, default=None):
        """Set default value if key doesn't exist."""
        cdef dict data

        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            data.setdefault(key, default)
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
        finally:
            self.lock.release()

    # Bulk operations
    cpdef void bulk_update(self, dict updates):
        """Bulk update multiple keys at once."""
        cdef dict data

        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            data.update(updates)
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
        finally:
            self.lock.release()

    cpdef list bulk_get(self, list keys):
        """Bulk get multiple keys."""
        cdef dict data = self._ensure_synced()
        return [data.get(key) for key in keys]

    # Atomic operations
    cpdef long increment(self, str key, long amount=1):
        """Atomically increment a numeric value."""
        cdef dict data
        cdef long current

        if key is None:
            raise ValueError("key must not be None")
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            assert isinstance(data, dict), "loaded state must be a dict"
            current = data.get(key, 0)
            if not isinstance(current, int):
                current = 0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
            # Postcondition: stored value reflects the returned counter.
            assert data[key] == current, "stored value must match returned value"
            return current
        finally:
            self.lock.release()

    cpdef double increment_float(self, str key, double amount=1.0):
        """Atomically increment a float value."""
        cdef dict data
        cdef double current

        if key is None:
            raise ValueError("key must not be None")
        if not self.lock.try_acquire(blocking=True, timeout=5.0):
            raise RuntimeError("Could not acquire lock for shared dictionary operation")

        try:
            data = self._load_from_redis()
            assert isinstance(data, dict), "loaded state must be a dict"
            current = data.get(key, 0.0)
            if not isinstance(current, (int, float)):
                current = 0.0
            current += amount
            data[key] = current
            self._save_to_redis(data)
            self.local_cache = data
            self.last_sync = <long>time.time()
            # Postcondition: stored value reflects the returned counter.
            assert data[key] == current, "stored value must match returned value"
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

        # Invariant: serialized size is at least '{}' and non-negative.
        assert total_size >= 2, "serialized size is at least '{}'"

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

    cdef object redis
    cdef dict dicts
    cdef object executor

    def __cinit__(self, redis_client):
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        self.redis = redis_client
        self.dicts = {}
        self.executor = ThreadPoolExecutor(max_workers=4)
        assert isinstance(self.dicts, dict), "dict registry must be a dict"

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef CySharedDict get_dict(self, str name, bint use_compression=True, long cache_ttl=30):
        """Get or create a shared dictionary."""
        if not name:
            raise ValueError("name must be a non-empty string")
        if cache_ttl < 0:
            raise ValueError("cache_ttl must be non-negative")

        if name not in self.dicts:
            self.dicts[name] = CySharedDict(self.redis, f"shared_dict:{name}",
                                           use_compression, cache_ttl)
        # Postcondition: a registered dict is always returned for this name.
        assert name in self.dicts, "dict must be registered after get_dict"
        assert isinstance(self.dicts[name], CySharedDict), "registry holds CySharedDict"
        return self.dicts[name]

    cpdef void delete_dict(self, str name):
        """Delete a shared dictionary."""
        cdef CySharedDict d

        if not name:
            raise ValueError("name must be a non-empty string")

        if name in self.dicts:
            # Clear Redis data
            d = self.dicts[name]
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
