# cython: language_level=3

"""
Web Cache Module Declarations for CyRedis
Provides FastAPI-cache inspired functionality declarations.
"""

# No cimport needed - using object types

# Forward declarations for Cython classes
cdef class HTTPCacheHeaders:
    cdef str etag_header_name
    cdef str cache_control_header

cdef class Coder:
    pass

cdef class JsonCoder(Coder):
    pass

cdef class PickleCoder(Coder):
    pass

cdef class KeyBuilder:
    pass

cdef class DefaultKeyBuilder(KeyBuilder):
    pass

cdef class RequestKeyBuilder(KeyBuilder):
    pass

cdef class CacheBackend:
    cdef str prefix

cdef class RedisCacheBackend(CacheBackend):
    cdef object redis_client  # CyRedisClient

cdef class InMemoryCacheBackend(CacheBackend):
    cdef dict cache
    cdef dict expiries

cdef class MemcachedCacheBackend(CacheBackend):
    cdef str host
    cdef int port
    cdef object _client

cdef class DynamoDBCacheBackend(CacheBackend):
    cdef str table_name
    cdef str region_name
    cdef object _client
    cdef object _table

cdef class CacheManager:
    cdef CacheBackend backend
    cdef Coder coder
    cdef KeyBuilder key_builder
    cdef str prefix
    cdef int default_ttl
    cdef HTTPCacheHeaders http_headers

# Factory functions
cdef CacheManager create_redis_cache(object redis_client, str prefix, int default_ttl)
cdef CacheManager create_memory_cache(str prefix, int default_ttl)

# Global functions
cdef WebCache init_cache(object redis_client, str backend_type, str prefix, int default_ttl)
cdef WebCache get_cache()

# Python wrapper class
cdef class WebCache:
    cdef CacheManager cache_manager
