# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Game Engine
"""

# Import base CyRedis components
from cy_redis.cy_redis_client cimport CyRedisClient
from cy_redis.functions cimport CyRedisFunctionsManager

# Constants
DEF DEFAULT_TICK_MS = 50
DEF MAX_INTENTS_PER_TICK = 256
DEF SPATIAL_PRECISION = 1000

# Zone class
cdef class CyZone:
    cdef str world_id
    cdef str zone_id
    cdef CyRedisClient redis
    cdef CyRedisFunctionsManager func_mgr

    cdef str tick_key
    cdef str intents_stream
    cdef str events_stream
    cdef str spatial_index
    cdef str schedule_zset

# World class
cdef class CyGameWorld:
    cdef str world_id
    cdef CyRedisClient redis
    cdef CyRedisFunctionsManager func_mgr
    cdef dict zones
    cdef str zones_list_key

# Game Engine
cdef class CyGameEngine:
    cdef CyRedisClient redis
    cdef CyRedisFunctionsManager func_mgr
    cdef dict worlds
    cdef object executor
