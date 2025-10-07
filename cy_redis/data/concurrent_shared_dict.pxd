# Concurrent Shared Dictionary declarations

cdef class ConcurrentSharedDict:
    cdef str dict_name
    cdef object redis_client
    cdef str dict_key
    cdef str lock_key
    cdef dict local_cache
    cdef int cache_ttl
    cdef long last_sync
    cdef object lock

    cdef dict _load_from_redis(self)
    cdef void _save_to_redis(self, dict data)
    cdef bint _is_cache_valid(self)
    cdef void _invalidate_cache(self)
    cdef dict _ensure_synced(self)
