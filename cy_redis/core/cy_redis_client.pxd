# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedisClient - High-performance Cython Redis client
"""

from libc.stdint cimport uint64_t

# System time struct
cdef extern from "sys/time.h":
    cdef struct timeval:
        long tv_sec
        long tv_usec

# hiredis structures - declared as extern at module level
cdef extern from "hiredis.h":
    ctypedef struct redisContext:
        int err
        char errstr[128]
        int fd
        int flags
        char *obuf
        void *reader
        int connection_type
        timeval *connect_timeout
        timeval *command_timeout
        void *privdata
        void (*free_privdata)(void *)
        void *privctx

    ctypedef struct redisReply:
        int type
        long long integer
        double dval
        size_t len
        char *str
        size_t elements
        redisReply **element

    # Core connection functions
    redisContext *redisConnect(const char *ip, int port)
    redisContext *redisConnectUnix(const char *path)
    void redisFree(redisContext *c)
    int redisReconnect(redisContext *c)

    # Command functions
    redisReply *redisCommand(redisContext *c, const char *format, ...)
    redisReply *redisCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen) nogil
    void freeReplyObject(redisReply *reply)

    # Pipeline functions
    int redisAppendCommand(redisContext *c, const char *format, ...)
    int redisAppendCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen)
    int redisGetReply(redisContext *c, void **reply) nogil

# Constants
DEF REDIS_REPLY_STRING = 1
DEF REDIS_REPLY_ARRAY = 2
DEF REDIS_REPLY_INTEGER = 3
DEF REDIS_REPLY_NIL = 4
DEF REDIS_REPLY_STATUS = 5
DEF REDIS_REPLY_ERROR = 6

DEF REDIS_OK = 0
DEF REDIS_ERR = -1

# Connection wrapper class - properly wraps hiredis redisContext
cdef class CyRedisConnection:
    cdef redisContext *ctx
    cdef bint _connected
    cdef str _host
    cdef int _port
    cdef double _timeout
    cdef int _protocol_version
    cdef str _password
    cdef int _db

    cdef int _connect(self)
    cdef bint _auth_and_select(self) except *
    cdef void _disconnect(self)
    cdef object _execute_raw(self, list args)
    cdef object _ensure_connected_and_run(self, list args)
    cpdef object _execute_command(self, list args)
    cdef object _parse_reply(self, redisReply *reply, int depth=*)

# Connection pool class - manages multiple CyRedisConnection instances
cdef class CyRedisConnectionPool:
    cdef object _connections
    cdef int _max_connections
    cdef int _in_use
    cdef int _total_created
    cdef object _lock
    cdef object _semaphore
    cdef double _wait_timeout
    cdef str _host
    cdef int _port
    cdef double _timeout
    cdef str _password
    cdef int _db

    cpdef CyRedisConnection get_connection(self)
    cpdef void return_connection(self, CyRedisConnection conn)

# Pipeline class - batches commands into one round trip
cdef class CyRedisPipeline:
    cdef CyRedisConnection _conn
    cdef CyRedisConnectionPool _pool
    cdef int _queued
    cdef list _buffer
    cdef list _transforms
    cdef bint _in_multi
    cdef bint _buffering

    cdef int _queue(self, list args, int transform) except -1
    cdef object _run_or_queue(self, list args, int transform)
    cdef int _append_one(self, list args) except -1
    cdef list _read_replies(self, int n, list transforms)

# Main client class - provides high-level Redis operations
cdef class CyRedisClient:
    cdef CyRedisConnectionPool _pool
    cdef object _executor
    cdef dict _stream_offsets
    cdef object _offset_lock
    cdef str _server_type   # "redis" | "valkey" | None (undetected)

    cdef list _parse_xread_result(self, list result)
    cdef void _negotiate_protocol(self)
    cdef str _parse_server_type(self, object info_response)
