"""
Thin Python wrapper for Cython distributed components.
Provides sync and async interfaces over optimized Cython implementations.
"""

import asyncio
from typing import Optional, Any, Dict, List
from contextlib import contextmanager

# Import optimized Cython implementations
try:
    from cy_redis.distributed import CyDistributedLock, CyReadWriteLock
    _OPTIMIZED_AVAILABLE = True
except ImportError:
    _OPTIMIZED_AVAILABLE = False


class DistributedLock:
    """
    Thin Python wrapper for CyDistributedLock.
    Provides sync and async interfaces with automatic fallback.
    """

    def __init__(self, redis_client, lock_key: str, lock_value: Optional[str] = None,
                 ttl_ms: int = 30000, retry_delay: float = 0.1, max_retries: int = 50):
        if _OPTIMIZED_AVAILABLE:
            self._impl = CyDistributedLock(redis_client.client, lock_key, lock_value, ttl_ms, retry_delay, max_retries)
            self._executor = None  # Not needed for sync operations
        else:
            raise RuntimeError("Optimized distributed components not available")

    def try_acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Sync lock acquisition"""
        return self._impl.try_acquire(blocking, timeout if timeout is not None else -1.0)

    def release(self):
        """Sync lock release"""
        self._impl.release()

    def is_locked(self) -> bool:
        """Check if lock is held"""
        return self._impl.is_locked()

    def get_ttl(self) -> float:
        """Get lock TTL"""
        return self._impl.get_ttl()

    def extend(self, ttl_ms: int = -1) -> bool:
        """Extend lock TTL"""
        return self._impl.extend(ttl_ms)

    @contextmanager
    def acquire(self, blocking: bool = True, timeout: Optional[float] = None):
        """Context manager for lock acquisition"""
        acquired = self.try_acquire(blocking, timeout)
        try:
            yield acquired
        finally:
            if acquired:
                self.release()

    # Async versions
    async def try_acquire_async(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Async lock acquisition"""
        return await self._impl.try_acquire_async(blocking, timeout if timeout is not None else -1.0)

    async def release_async(self):
        """Async lock release"""
        await self._impl.release_async()

    async def extend_async(self, ttl_ms: int = -1) -> bool:
        """Async lock extension"""
        return await self._impl.extend_async(ttl_ms)


class ReadWriteLock:
    """
    Thin Python wrapper for CyReadWriteLock.
    Provides sync and async interfaces with automatic fallback.
    """

    def __init__(self, redis_client, lock_key: str, ttl_ms: int = 30000):
        if _OPTIMIZED_AVAILABLE:
            self._impl = CyReadWriteLock(redis_client.client, lock_key, ttl_ms)
            self._executor = None  # Not needed for sync operations
        else:
            raise RuntimeError("Optimized distributed components not available")

    def try_read_lock(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Try to acquire a read lock"""
        return self._impl.try_read_lock(blocking, timeout if timeout is not None else -1.0)

    def try_write_lock(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Try to acquire a write lock"""
        return self._impl.try_write_lock(blocking, timeout if timeout is not None else -1.0)

    def release_read_lock(self):
        """Release a read lock"""
        self._impl.release_read_lock()

    def release_write_lock(self):
        """Release a write lock"""
        self._impl.release_write_lock()

    def get_stats(self) -> Dict:
        """Get lock statistics"""
        return self._impl.get_stats()

    # Async versions
    async def try_read_lock_async(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Async read lock acquisition"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.try_read_lock, blocking, timeout)

    async def try_write_lock_async(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Async write lock acquisition"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.try_write_lock, blocking, timeout)

    async def release_read_lock_async(self):
        """Async read lock release"""
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.release_read_lock)

    async def release_write_lock_async(self):
        """Async write lock release"""
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.release_write_lock)


# Additional distributed components will be added here...
# Semaphore, DistributedCounter, LeaderElection
