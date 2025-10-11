# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedisClient - High-performance Cython Redis client
"""

from libc.stdint cimport uint64_t

# System time struct
cdef extern from "sys/time.h":
    ctypedef struct timeval:
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
        struct timeval *connect_timeout
        struct timeval *command_timeout
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
    redisContext *redisConnectWithTimeout(const char *ip, int port, struct timeval tv)
    redisContext *redisConnectUnix(const char *path)
    void redisFree(redisContext *c)
    int redisReconnect(redisContext *c)

    # Command functions
    redisReply *redisCommand(redisContext *c, const char *format, ...)
    redisReply *redisCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen)
    void freeReplyObject(redisReply *reply)

    # Pipeline functions
    int redisAppendCommand(redisContext *c, const char *format, ...)
    int redisAppendCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen)
    int redisGetReply(redisContext *c, void **reply)

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
    cdef bint connected
    cdef str host
    cdef int port
    cdef double timeout

    cdef int connect(self)
    cdef void disconnect(self)
    cdef object execute_command(self, list args)
    cdef object _parse_reply(self, redisReply *reply)

# Connection pool class - manages multiple CyRedisConnection instances
cdef class CyRedisConnectionPool:
    cdef object connections  # List of CyRedisConnection instances
    cdef int max_connections
    cdef object lock
    cdef str host
    cdef int port
    cdef double timeout

    cdef CyRedisConnection get_connection(self)
    cdef void return_connection(self, CyRedisConnection conn)

# Main client class - provides high-level Redis operations
cdef class CyRedisClient:
    cdef CyRedisConnectionPool pool
    cdef object executor
    cdef dict stream_offsets
    cdef object offset_lock

    cdef list _parse_xread_result(self, list result)

    # Lua script support
    cpdef object eval(self, str script, list keys=?, list args=?)
    cpdef object evalsha(self, str sha, list keys=?, list args=?)
    cpdef str script_load(self, str script)
    cpdef str script_kill(self)
    cpdef str script_flush(self)
    cpdef list script_exists(self, list shas)
