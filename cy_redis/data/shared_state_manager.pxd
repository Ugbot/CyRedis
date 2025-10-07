# Shared State Manager declarations

cdef class SharedStateManager:
    cdef object redis_client
    cdef str locks_key
    cdef str counters_key
    cdef str data_key
