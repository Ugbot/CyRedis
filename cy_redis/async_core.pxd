# cython: language_level=3
# distutils: language=c

"""
Header declarations for AsyncRedisCore
"""

from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int32_t, int64_t

# Forward declarations
cdef class AsyncMessageQueue
cdef class AsyncRedisConnection
cdef class AsyncRedisClient

# C-level structures
cdef struct AsyncMessage:
    int msg_type
    void *data
    size_t data_len
    uint64_t timestamp
    void *callback
    void *user_data

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

cdef struct MessageQueue:
    AsyncMessage *messages
    size_t capacity
    size_t head
    size_t tail
    size_t count
    void *lock  # pthread_mutex_t
    void *not_empty  # pthread_cond_t
    void *not_full  # pthread_cond_t

# Class declarations
cdef class AsyncMessageQueue:
    cdef MessageQueue *queue

cdef class AsyncRedisConnection:
    cdef ConnectionState *state
    cdef AsyncMessageQueue *command_queue
    cdef AsyncMessageQueue *response_queue
    cdef object loop
    cdef object thread
    cdef bint running

    cdef int connect(self) nogil
    cdef int disconnect(self) nogil
    cdef int send_command(self, const char *command, size_t cmd_len) nogil
    cdef object _parse_reply(self, void *reply)

cdef class AsyncRedisClient:
    cdef:
        AsyncRedisConnection **connections
        size_t num_connections
        size_t max_connections
        AsyncMessageQueue *work_queue
        void **worker_threads
        size_t num_worker_threads
        bint running

    cdef AsyncRedisConnection* get_connection(self)
    cdef void _process_command(self, AsyncMessage *msg) nogil
