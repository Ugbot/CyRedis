# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Functions Library
"""

from cy_redis.cy_redis_client cimport CyRedisClient

# Function manager
cdef class CyRedisFunctionsManager:
    cdef CyRedisClient redis
    cdef dict loaded_libraries
    cdef object executor

# Domain-specific classes
cdef class CyLocks:
    cdef CyRedisFunctionsManager func_mgr

cdef class CyRateLimiter:
    cdef CyRedisFunctionsManager func_mgr

cdef class CyQueue:
    cdef CyRedisFunctionsManager func_mgr
