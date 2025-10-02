# cython: language_level=3
# distutils: language=c

"""
Header declarations for RedisCore - Complete Cython Redis client
"""

from libc.stdint cimport uint64_t

# C-level structures
cdef struct AsyncMessage:
    int msg_type
    void *data
    size_t data_len
    uint64_t timestamp
    void *callback
    void *user_data

cdef struct QueueStruct:
    AsyncMessage *messages
    size_t capacity
    size_t head
    size_t tail
    size_t count
    void *lock  # pthread_mutex_t
    void *not_empty  # pthread_cond_t
    void *not_full  # pthread_cond_t

cdef struct ConnectionState:
    int state
    void *ctx  # redisContext*
    char *host
    int port
    int fd
    uint64_t last_activity
    int retry_count
    char *buffer
    size_t buffer_size
    size_t buffer_used

# Class declarations
cdef class MessageQueue:
    cdef QueueStruct *queue

    cdef int enqueue(self, AsyncMessage *msg) nogil
    cdef int dequeue(self, AsyncMessage *msg) nogil

cdef class RedisConnection:
    cdef ConnectionState *state

    cdef int connect(self) nogil
    cdef int disconnect(self) nogil
    cdef object execute_command(self, list args)
    cdef object _parse_reply(self, void *reply)

cdef class ConnectionPool:
    cdef list connections
    cdef int max_connections
    cdef object lock
    cdef str host
    cdef int port
    cdef double timeout

    cdef RedisConnection get_connection(self)
    cdef void return_connection(self, RedisConnection conn)

cdef class CyRedisClient:
    cdef ConnectionPool pool
    cdef object executor
    cdef dict stream_offsets
    cdef object offset_lock

    cdef list _parse_xread_result(self, list result)
