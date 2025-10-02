# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
High-Performance Cython Distributed Systems Components
Optimized implementations of distributed locks, counters, semaphores, and primitives.
"""

import time
import uuid
from typing import Optional, Any, Dict, List
from contextlib import contextmanager

# Import our optimized Redis client
from cy_redis.cy_redis_client import CyRedisClient


# Optimized Distributed Lock
cdef class CyDistributedLock:
    """
    High-performance distributed lock using Redis SET NX operations.
    Optimized for low-latency lock acquisition and release.
    """

    def __cinit__(self, redis_client, str lock_key,
                  str lock_value=None, int ttl_ms=30000,
                  double retry_delay=0.1, int max_retries=50):
        self.redis = redis_client
        self.lock_key = lock_key
        self.lock_value = lock_value or f"lock:{time.time()}:{str(uuid.uuid4())[:8]}"
        self.ttl_ms = ttl_ms
        self.retry_delay = retry_delay
        self.max_retries = max_retries

    cpdef bint try_acquire(self, bint blocking=True, double timeout=-1.0):
        """
        Try to acquire the lock with optimized Redis operations.

        Args:
            blocking: Whether to block until lock is acquired
            timeout: Maximum time to wait for lock (-1 = no timeout)

        Returns:
            True if lock acquired, False otherwise
        """
        cdef double start_time = time.time()
        cdef int attempts = 0
        cdef double elapsed = 0.0

        while attempts < self.max_retries:
            # Try SET NX with TTL
            result = self.redis.execute_command([
                'SET', self.lock_key, self.lock_value, 'NX', 'PX', str(self.ttl_ms)
            ])

            if result == 'OK':
                return True

            if not blocking:
                return False

            # Check timeout
            if timeout > 0:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            attempts += 1
            time.sleep(self.retry_delay)

        return False

    cpdef void release(self):
        """
        Release the lock using Lua script for atomicity.
        Only releases if we own the lock.
        """
        # Use Lua script for atomic check-and-delete
        lua_script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """

        try:
            self.redis.execute_command(['EVAL', lua_script, '1', self.lock_key, self.lock_value])
        except Exception as e:
            print(f"Error releasing lock {self.lock_key}: {e}")

    cpdef bint is_locked(self):
        """Check if the lock is currently held"""
        try:
            result = self.redis.get(self.lock_key)
            return result is not None
        except Exception:
            return False

    cpdef double get_ttl(self):
        """Get remaining TTL of the lock in milliseconds"""
        try:
            result = self.redis.execute_command(['PTTL', self.lock_key])
            return result if result and result > 0 else 0.0
        except Exception:
            return 0.0

    cpdef bint extend(self, int ttl_ms=-1):
        """
        Extend the lock TTL if we own it.

        Args:
            ttl_ms: New TTL in milliseconds (-1 = use current TTL)

        Returns:
            True if extended successfully
        """
        if ttl_ms == -1:
            ttl_ms = self.ttl_ms

        lua_script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        try:
            result = self.redis.execute_command(['EVAL', lua_script, '1', self.lock_key, self.lock_value, str(ttl_ms)])
            return result == 1
        except Exception as e:
            print(f"Error extending lock {self.lock_key}: {e}")
            return False

    # Context manager support
    def __enter__(self):
        self.try_acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    # Async versions
    async def try_acquire_async(self, blocking: bool = True, timeout: float = -1.0):
        """Async lock acquisition"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.try_acquire, blocking, timeout)

    async def release_async(self):
        """Async lock release"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.release)

    async def extend_async(self, ttl_ms: int = -1):
        """Async lock extension"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.extend, ttl_ms)


# Optimized Read-Write Lock
cdef class CyReadWriteLock:
    """
    Distributed read-write lock allowing multiple readers or single writer.
    Optimized for high-throughput read operations.
    """

    def __cinit__(self, redis_client, str lock_key,
                  int ttl_ms=30000):
        self.redis = redis_client
        self.base_key = lock_key
        self.read_key = f"{lock_key}:read"
        self.write_key = f"{lock_key}:write"
        self.ttl_ms = ttl_ms
        self.client_id = f"client:{time.time()}:{str(uuid.uuid4())[:8]}"

    cpdef bint try_read_lock(self, bint blocking=True, double timeout=-1.0):
        """
        Try to acquire a read lock.
        Multiple readers can hold read locks simultaneously.
        """
        cdef double start_time = time.time()

        while True:
            # Check if write lock is held
            write_owner = self.redis.get(self.write_key)
            if write_owner is not None and write_owner != self.client_id:
                if not blocking:
                    return False

                if timeout > 0 and (time.time() - start_time) >= timeout:
                    return False

                time.sleep(0.01)  # Small delay
                continue

            # Try to increment read counter
            result = self.redis.execute_command(['HINCRBY', self.read_key, self.client_id, '1'])

            if result is not None:
                # Extend TTL
                self.redis.execute_command(['PEXPIRE', self.read_key, str(self.ttl_ms)])
                return True

            if not blocking:
                return False

            if timeout > 0 and (time.time() - start_time) >= timeout:
                return False

            time.sleep(0.01)

        return False

    cpdef bint try_write_lock(self, bint blocking=True, double timeout=-1.0):
        """
        Try to acquire a write lock.
        Only one writer can hold the write lock, and no readers can hold read locks.
        """
        cdef double start_time = time.time()

        while True:
            # Check if any readers exist
            readers = self.redis.execute_command(['HLEN', self.read_key])
            if readers and readers > 0:
                if not blocking:
                    return False

                if timeout > 0 and (time.time() - start_time) >= timeout:
                    return False

                time.sleep(0.01)
                continue

            # Try to set write lock
            result = self.redis.execute_command([
                'SET', self.write_key, self.client_id, 'NX', 'PX', str(self.ttl_ms)
            ])

            if result == 'OK':
                return True

            if not blocking:
                return False

            if timeout > 0 and (time.time() - start_time) >= timeout:
                return False

            time.sleep(0.01)

        return False

    cpdef void release_read_lock(self):
        """Release a read lock"""
        lua_script = """
        local count = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
        if count <= 0 then
            redis.call('HDEL', KEYS[1], ARGV[1])
        end
        if redis.call('HLEN', KEYS[1]) == 0 then
            redis.call('DEL', KEYS[1])
        end
        return count
        """

        try:
            self.redis.execute_command(['EVAL', lua_script, '1', self.read_key, self.client_id])
        except Exception as e:
            print(f"Error releasing read lock {self.base_key}: {e}")

    cpdef void release_write_lock(self):
        """Release a write lock"""
        lua_script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """

        try:
            self.redis.execute_command(['EVAL', lua_script, '1', self.write_key, self.client_id])
        except Exception as e:
            print(f"Error releasing write lock {self.base_key}: {e}")

    cpdef dict get_stats(self):
        """Get lock statistics"""
        try:
            readers = self.redis.execute_command(['HLEN', self.read_key]) or 0
            writer = self.redis.get(self.write_key)
            return {
                'readers': readers,
                'writer': writer is not None,
                'writer_id': writer
            }
        except Exception:
            return {'readers': 0, 'writer': False, 'writer_id': None}


# Additional classes will be implemented...
# CySemaphore, CyDistributedCounter, CyLeaderElection

# Import asyncio for async methods
import asyncio
