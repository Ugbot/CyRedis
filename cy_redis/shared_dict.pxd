# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Shared Dictionary
"""

# Forward declarations
cdef class CyRedisClient
cdef class CyDistributedLock

# Shared Dictionary
cdef class CySharedDict:
    cdef object redis  # CyRedisClient
    cdef str dict_key
    cdef str lock_key
    cdef object lock  # CyDistributedLock
    cdef bint use_compression
    cdef object executor
    cdef dict local_cache
    cdef long cache_ttl
    cdef long last_sync

# Shared Dictionary Manager
cdef class CySharedDictManager:
    cdef object redis  # CyRedisClient
    cdef dict dicts
    cdef object executor
