# Redis Iterators declarations

cdef class RedisStreamIterator:
    cdef object redis_client
    cdef str stream_key
    cdef str consumer_group
    cdef str consumer_name
    cdef int batch_size
    cdef int block_ms
    cdef str last_id
    cdef bint is_closed

cdef class RedisListIterator:
    cdef object redis_client
    cdef str list_key
    cdef int batch_size
    cdef int block_ms
    cdef bint is_closed
    cdef long last_index

cdef class RedisPubSubIterator:
    cdef object redis_client
    cdef list channels
    cdef int timeout_ms
    cdef bint is_closed
    cdef object pubsub
