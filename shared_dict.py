#!/usr/bin/env python3
"""
Shared Dictionary - Replicated dictionary via Redis with concurrency control
"""

from typing import Any, Optional, Dict, List, Iterator, Union, Tuple, Iterable

try:
    from cy_redis.shared_dict import CySharedDict, CySharedDictManager
    _SHARED_DICT_AVAILABLE: bool = True
    print("✓ Using optimized Cython shared dictionary")
except ImportError:
    _SHARED_DICT_AVAILABLE = False
    print("⚠️  Optimized shared dictionary not available, using fallback")

from optimized_redis import OptimizedRedis


class SharedDict:
    """
    Shared dictionary replicated via Redis with concurrency control.

    Features:
    - Automatic Redis replication
    - Distributed locking for consistency
    - Local caching for performance
    - Compression for large data
    - Atomic operations
    - Bulk operations
    """

    def __init__(self, redis_client: Optional[OptimizedRedis] = None, dict_key: str = "shared_dict", use_compression: bool = True, cache_ttl: int = 30) -> None:
        if redis_client is None:
            redis_client = OptimizedRedis()

        if _SHARED_DICT_AVAILABLE:
            # Use optimized Cython implementation
            self._impl = CySharedDict(redis_client.client, dict_key, use_compression, cache_ttl)
        else:
            # Fallback implementation (would need to be implemented)
            raise RuntimeError("Shared dictionary requires Cython implementation")

    # Dictionary interface
    def __getitem__(self, key: Any) -> Any:
        return self._impl[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._impl[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._impl[key]

    def __contains__(self, key: Any) -> bool:
        return key in self._impl

    def __len__(self) -> int:
        return len(self._impl)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._impl)

    def keys(self) -> Iterator[Any]:
        return self._impl.keys()

    def values(self) -> Iterator[Any]:
        return self._impl.values()

    def items(self) -> Iterator[Tuple[Any, Any]]:
        return self._impl.items()

    def get(self, key: Any, default: Any = None) -> Any:
        return self._impl.get(key, default)

    def update(self, other: Union[Dict[Any, Any], Iterable[Tuple[Any, Any]]]) -> None:
        self._impl.update(other)

    def clear(self) -> None:
        self._impl.clear()

    def pop(self, key: Any, default: Any = None) -> Any:
        return self._impl.pop(key, default)

    def popitem(self) -> Tuple[Any, Any]:
        return self._impl.popitem()

    def setdefault(self, key: Any, default: Any = None) -> Any:
        return self._impl.setdefault(key, default)

    # Advanced operations
    def bulk_update(self, updates: Dict[Any, Any]) -> None:
        """Bulk update multiple keys."""
        self._impl.bulk_update(updates)

    def bulk_get(self, keys: List[Any]) -> Dict[Any, Any]:
        """Bulk get multiple keys."""
        return self._impl.bulk_get(keys)

    def increment(self, key: Any, amount: int = 1) -> int:
        """Atomically increment a numeric value."""
        return self._impl.increment(key, amount)

    def increment_float(self, key: Any, amount: float = 1.0) -> float:
        """Atomically increment a float value."""
        return self._impl.increment_float(key, amount)

    # Synchronization
    def sync(self) -> None:
        """Force synchronization with Redis."""
        self._impl.sync()

    def is_synced(self) -> bool:
        """Check if local cache is synchronized."""
        return self._impl.is_synced()

    def get_stats(self) -> Dict[str, Any]:
        """Get dictionary statistics."""
        return self._impl.get_stats()

    def copy(self) -> 'SharedDict':
        """Create a copy of the dictionary."""
        return self._impl.copy()

    def __repr__(self) -> str:
        return repr(self._impl)


class SharedDictManager:
    """
    Manager for multiple shared dictionaries with connection pooling.
    """

    def __init__(self, redis_client: Optional[OptimizedRedis] = None) -> None:
        if redis_client is None:
            redis_client = OptimizedRedis()

        if _SHARED_DICT_AVAILABLE:
            self._impl = CySharedDictManager(redis_client.client)
        else:
            raise RuntimeError("Shared dictionary manager requires Cython implementation")

    def get_dict(self, name: str, use_compression: bool = True, cache_ttl: int = 30) -> SharedDict:
        """Get or create a shared dictionary."""
        cdef_dict = self._impl.get_dict(name, use_compression, cache_ttl)
        # Wrap in Python class
        result = SharedDict.__new__(SharedDict)
        result._impl = cdef_dict
        return result

    def delete_dict(self, name: str) -> None:
        """Delete a shared dictionary."""
        self._impl.delete_dict(name)

    def list_dicts(self) -> List[str]:
        """List all managed dictionaries."""
        return self._impl.list_dicts()

    def get_global_stats(self) -> Dict[str, Any]:
        """Get statistics for all managed dictionaries."""
        return self._impl.get_global_stats()


# Convenience functions
def create_shared_dict(name: str = "default", **kwargs: Any) -> SharedDict:
    """Create a shared dictionary."""
    return SharedDict(dict_key=f"shared_dict:{name}", **kwargs)

def create_shared_dict_manager(**kwargs: Any) -> SharedDictManager:
    """Create a shared dictionary manager."""
    return SharedDictManager(**kwargs)
