# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Distributed Components
"""

# No forward declarations needed - using object types

# Distributed Lock
cdef class CyDistributedLock:
    cdef object redis  # CyRedisClient
    cdef str lock_key
    cdef str lock_value
    cdef int ttl_ms
    cdef double retry_delay
    cdef int max_retries

    cpdef bint try_acquire(self, bint blocking=?, double timeout=?)
    cpdef void release(self)
    cpdef bint is_locked(self)
    cpdef double get_ttl(self)
    cpdef bint extend(self, int ttl_ms=?)

# Read-Write Lock
cdef class CyReadWriteLock:
    cdef object redis  # CyRedisClient
    cdef str base_key
    cdef str read_key
    cdef str write_key
    cdef int ttl_ms
    cdef str client_id

    cpdef bint try_read_lock(self, bint blocking=?, double timeout=?)
    cpdef bint try_write_lock(self, bint blocking=?, double timeout=?)
    cpdef void release_read_lock(self)
    cpdef void release_write_lock(self)
    cpdef dict get_stats(self)

# Additional classes will be declared here as they are implemented
