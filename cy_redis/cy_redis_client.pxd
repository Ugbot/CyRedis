# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedisClient - High-performance Cython Redis client
"""

from libc.stdint cimport uint64_t

# Hiredis includes
cdef extern from "hiredis/hiredis.h":
    ctypedef struct redisContext:
        int err
        char errstr[128]
        int fd
        int flags
        char *obuf
        redisReader *reader

    ctypedef struct redisReply:
        int type
        long long integer
        double dval
        size_t len
        char *str
        size_t elements
        redisReply **element

cdef extern from "hiredis/read.h":
    ctypedef struct redisReader:
        pass

cdef extern from "sys/time.h":
    struct timeval:
        long tv_sec
        long tv_usec

# Standard library functions
cdef extern from "stdlib.h":
    void *malloc(size_t size)
    void free(void *ptr)

# Hiredis function declarations
cdef extern from "hiredis/hiredis.h":
    redisContext *redisConnect(const char *ip, int port)
    redisContext *redisConnectWithTimeout(const char *ip, int port, const timeval tv)
    redisContext *redisConnectUnix(const char *path)
    void redisFree(redisContext *c)
    int redisReconnect(redisContext *c)
    redisReply *redisCommand(redisContext *c, const char *format, ...)
    redisReply *redisCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen)
    void freeReplyObject(redisReply *reply)

# Constants
DEF REDIS_REPLY_STRING = 1
DEF REDIS_REPLY_ARRAY = 2
DEF REDIS_REPLY_INTEGER = 3
DEF REDIS_REPLY_NIL = 4
DEF REDIS_REPLY_STATUS = 5
DEF REDIS_REPLY_ERROR = 6

# Class declarations
cdef class CyRedisConnection:
    cdef redisContext *ctx
    cdef bint connected
    cdef str host
    cdef int port
    cdef double timeout

    cdef int connect(self)
    cdef void disconnect(self)
    cpdef object _execute_command(self, list args)
    def execute_command(self, list args)
    cdef object _parse_reply(self, redisReply *reply)

cdef class CyRedisConnectionPool:
    cdef list connections
    cdef int max_connections
    cdef object lock
    cdef str host
    cdef int port
    cdef double timeout

    cdef CyRedisConnection get_connection(self)
    cdef void return_connection(self, CyRedisConnection conn)

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
