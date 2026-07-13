# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
CyRedis Core - Complete high-performance Redis client in Cython
All Redis operations go through hiredis for maximum performance.
"""

import asyncio
import json
import threading
import time

from cpython.exc cimport PyErr_CheckSignals
from cpython.ref cimport Py_DECREF, Py_INCREF, PyObject
from libc.errno cimport errno
from libc.stdint cimport int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport free, malloc, realloc
from libc.string cimport memcpy, memset, strcpy, strdup, strlen


# Hiredis includes
cdef extern from "hiredis.h":
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

    redisContext *redisConnect(const char *ip, int port)
    redisContext *redisConnectUnix(const char *path)
    void redisFree(redisContext *c)
    int redisReconnect(redisContext *c)
    redisReply *redisCommand(redisContext *c, const char *format, ...)
    redisReply *redisCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen)
    void freeReplyObject(redisReply *reply)

cdef extern from "hiredis/read.h":
    ctypedef struct redisReader:
        pass

    redisReader *redisReaderCreate()
    void redisReaderFree(redisReader *reader)
    int redisReaderFeed(redisReader *reader, const char *buf, size_t len)
    int redisReaderGetReply(redisReader *reader, void **reply)

cdef extern from "sys/time.h":
    ctypedef struct timeval:
        long tv_sec
        long tv_usec

# Pthread includes for thread-safe operations
cdef extern from "<pthread.h>" nogil:
    ctypedef struct pthread_mutex_t:
        pass
    ctypedef struct pthread_cond_t:
        pass

    int pthread_mutex_init(pthread_mutex_t *, void *)
    int pthread_mutex_destroy(pthread_mutex_t *)
    int pthread_mutex_lock(pthread_mutex_t *)
    int pthread_mutex_unlock(pthread_mutex_t *)
    int pthread_cond_init(pthread_cond_t *, void *)
    int pthread_cond_destroy(pthread_cond_t *)
    int pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *)
    int pthread_cond_signal(pthread_cond_t *)

# Constants
DEF BUFFER_SIZE = 65536
DEF MAX_RETRIES = 3
DEF CONNECTION_TIMEOUT = 5000  # milliseconds
DEF COMMAND_TIMEOUT = 1000     # milliseconds

# Redis reply types
DEF REDIS_REPLY_STRING = 1
DEF REDIS_REPLY_ARRAY = 2
DEF REDIS_REPLY_INTEGER = 3
DEF REDIS_REPLY_NIL = 4
DEF REDIS_REPLY_STATUS = 5
DEF REDIS_REPLY_ERROR = 6

# Connection states
DEF CONN_DISCONNECTED = 0
DEF CONN_CONNECTING = 1
DEF CONN_CONNECTED = 2
DEF CONN_ERROR = 3

# Hard upper bound on RESP reply nesting depth. Real Redis replies nest only a
# few levels; this cap turns a pathological/hostile deeply-nested reply into a
# bounded, loud failure instead of unbounded recursion (TigerStyle: bound
# everything; no unbounded recursion). Matches cy_redis_client.pyx.
DEF MAX_REPLY_DEPTH = 128

# Message types for internal communication
DEF MSG_COMMAND = 1
DEF MSG_RESPONSE = 2
DEF MSG_ERROR = 3
DEF MSG_CLOSE = 4

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
    pthread_mutex_t lock
    pthread_cond_t not_empty
    pthread_cond_t not_full

cdef struct ConnectionState:
    int state
    redisContext *ctx
    char *host
    int port
    int fd
    uint64_t last_activity
    int retry_count
    char *buffer
    size_t buffer_size
    size_t buffer_used

# Thread-safe message queue
cdef class MessageQueue:
    cdef QueueStruct *queue

    def __cinit__(self, size_t capacity=1024):
        self.queue = <QueueStruct*>malloc(sizeof(QueueStruct))
        if self.queue == NULL:
            raise MemoryError("Failed to allocate message queue")

        memset(self.queue, 0, sizeof(QueueStruct))
        self.queue.capacity = capacity
        self.queue.messages = <AsyncMessage*>malloc(sizeof(AsyncMessage) * capacity)
        if self.queue.messages == NULL:
            free(self.queue)
            raise MemoryError("Failed to allocate message buffer")

        self.queue.head = 0
        self.queue.tail = 0
        self.queue.count = 0

        # Initialize mutex and condition variables
        cdef int result
        result = pthread_mutex_init(&self.queue.lock, NULL)
        if result != 0:
            free(self.queue.messages)
            free(self.queue)
            raise RuntimeError("Failed to initialize mutex")

        result = pthread_cond_init(&self.queue.not_empty, NULL)
        if result != 0:
            pthread_mutex_destroy(&self.queue.lock)
            free(self.queue.messages)
            free(self.queue)
            raise RuntimeError("Failed to initialize not_empty condition")

        result = pthread_cond_init(&self.queue.not_full, NULL)
        if result != 0:
            pthread_cond_destroy(&self.queue.not_empty)
            pthread_mutex_destroy(&self.queue.lock)
            free(self.queue.messages)
            free(self.queue)
            raise RuntimeError("Failed to initialize not_full condition")

    def __dealloc__(self):
        if self.queue != NULL:
            pthread_cond_destroy(&self.queue.not_full)
            pthread_cond_destroy(&self.queue.not_empty)
            pthread_mutex_destroy(&self.queue.lock)
            free(self.queue.messages)
            free(self.queue)

    cdef int enqueue(self, AsyncMessage *msg) nogil:
        """Thread-safe enqueue operation"""
        cdef int result = pthread_mutex_lock(&self.queue.lock)
        if result != 0:
            return -1

        # Wait for space if queue is full
        while self.queue.count >= self.queue.capacity:
            result = pthread_cond_wait(&self.queue.not_full, &self.queue.lock)
            if result != 0:
                pthread_mutex_unlock(&self.queue.lock)
                return -1

        # Copy message to queue
        memcpy(&self.queue.messages[self.queue.tail], msg, sizeof(AsyncMessage))
        self.queue.tail = (self.queue.tail + 1) % self.queue.capacity
        self.queue.count += 1

        # Signal that queue is not empty
        pthread_cond_signal(&self.queue.not_empty)
        pthread_mutex_unlock(&self.queue.lock)
        return 0

    cdef int dequeue(self, AsyncMessage *msg) nogil:
        """Thread-safe dequeue operation"""
        cdef int result = pthread_mutex_lock(&self.queue.lock)
        if result != 0:
            return -1

        # Wait for message if queue is empty
        while self.queue.count == 0:
            result = pthread_cond_wait(&self.queue.not_empty, &self.queue.lock)
            if result != 0:
                pthread_mutex_unlock(&self.queue.lock)
                return -1

        # Copy message from queue
        memcpy(msg, &self.queue.messages[self.queue.head], sizeof(AsyncMessage))
        self.queue.head = (self.queue.head + 1) % self.queue.capacity
        self.queue.count -= 1

        # Signal that queue is not full
        pthread_cond_signal(&self.queue.not_full)
        pthread_mutex_unlock(&self.queue.lock)
        return 0

class _RedisStateProxy:
    """Python proxy providing access to C-level connection state fields."""
    __slots__ = ['state']
    def __init__(self, s):
        self.state = s

# Redis connection with hiredis
cdef class RedisConnection:
    cdef ConnectionState *_cs

    @property
    def state(self):
        """Python-accessible connection state proxy."""
        return _RedisStateProxy(self._cs.state)

    def __cinit__(self, str host="localhost", int port=6379):
        self._cs = <ConnectionState*>malloc(sizeof(ConnectionState))
        if self._cs == NULL:
            raise MemoryError("Failed to allocate connection state")

        memset(self._cs, 0, sizeof(ConnectionState))
        self._cs.state = CONN_DISCONNECTED
        self._cs.host = strdup(host.encode('utf-8'))
        self._cs.port = port
        self._cs.buffer_size = BUFFER_SIZE
        self._cs.buffer = <char*>malloc(BUFFER_SIZE)
        if self._cs.buffer == NULL:
            free(self._cs)
            raise MemoryError("Failed to allocate buffer")

    def __dealloc__(self):
        if self._cs != NULL:
            if self._cs.ctx != NULL:
                redisFree(self._cs.ctx)
            if self._cs.host != NULL:
                free(self._cs.host)
            if self._cs.buffer != NULL:
                free(self._cs.buffer)
            free(self._cs)

    cdef int _connect(self):
        """Establish connection to Redis server using hiredis"""
        # Invariants set at construction: state struct is live, host is set, and
        # a TCP port is in 1..65535. Guard the impossible before a bad connect.
        assert self._cs != NULL, "connect on a freed connection"
        assert self._cs.host != NULL, "host must be set"
        assert 0 < self._cs.port <= 65535, "port out of range"
        self._cs.ctx = redisConnect(self._cs.host, self._cs.port)
        if self._cs.ctx == NULL or self._cs.ctx.err:
            self._cs.state = CONN_ERROR
            return -1

        self._cs.state = CONN_CONNECTED
        self._cs.fd = self._cs.ctx.fd
        self._cs.last_activity = <uint64_t>(time.time() * 1000000)
        # Postcondition: a successful connect leaves us live with a context.
        assert self._cs.ctx != NULL, "connected without a context"
        return 0

    def connect(self):
        """Python-accessible connect"""
        if self._connect() != 0:
            raise ConnectionError("Failed to connect to Redis")

    cdef int _disconnect(self):
        """Disconnect from Redis server"""
        if self._cs.ctx != NULL:
            redisFree(self._cs.ctx)
            self._cs.ctx = NULL

        self._cs.state = CONN_DISCONNECTED
        return 0

    def disconnect(self):
        """Python-accessible disconnect"""
        self._disconnect()

    def execute_command(self, args):
        """Python-accessible execute_command"""
        return self._execute_command(list(args))

    cdef object _execute_command(self, list args):
        """Execute Redis command using hiredis"""
        # Precondition: connection state must be allocated, and a command always
        # carries at least its name. Both are programmer errors if violated.
        assert self._cs != NULL, "execute on a freed connection"
        assert len(args) > 0, "execute requires at least the command name"
        if self._cs.state != CONN_CONNECTED:
            if self._connect() != 0:
                raise ConnectionError("Failed to connect to Redis")

        cdef int argc = len(args)
        cdef char **argv = <char **>malloc(sizeof(char *) * argc)
        cdef size_t *argvlen = <size_t *>malloc(sizeof(size_t) * argc)
        cdef redisReply *reply
        cdef list arg_bytes_list = []
        cdef int i
        cdef bytes arg_bytes

        if argv == NULL or argvlen == NULL:
            if argv != NULL: free(argv)
            if argvlen != NULL: free(argvlen)
            raise MemoryError("Failed to allocate command args")

        try:
            for i in range(argc):
                if isinstance(args[i], str):
                    arg_bytes = (<str>args[i]).encode('utf-8')
                elif isinstance(args[i], bytes):
                    arg_bytes = <bytes>args[i]
                else:
                    arg_bytes = str(args[i]).encode('utf-8')
                arg_bytes_list.append(arg_bytes)
                argv[i] = <char *>arg_bytes_list[i]
                argvlen[i] = len(arg_bytes_list[i])

            # Postcondition of the argv build: every slot was populated.
            assert len(arg_bytes_list) == argc, "argv build dropped an argument"
            reply = redisCommandArgv(self._cs.ctx, argc,
                                     <const char **>argv, <const size_t *>argvlen)
            if reply == NULL:
                raise ConnectionError("Redis command failed")

            try:
                result = self._parse_reply(reply)
                self._cs.last_activity = <uint64_t>(time.time() * 1000000)
                return result
            finally:
                freeReplyObject(reply)
        finally:
            free(argv)
            free(argvlen)

    cdef object _parse_reply(self, redisReply *reply, int depth=0):
        """Parse Redis reply into Python object.

        Recurses for aggregate types; `depth` bounds the nesting so a hostile
        or corrupt reply cannot drive unbounded recursion.
        """
        # Precondition: hiredis never hands us a NULL reply here — callers
        # branch on NULL before parsing — and nesting stays within bounds.
        assert reply != NULL, "parse called with NULL reply"
        assert 0 <= depth <= MAX_REPLY_DEPTH, "RESP reply nesting too deep"

        cdef list result
        cdef size_t i
        if reply.type == REDIS_REPLY_STRING:
            # Slice to reply.len and decode with errors='replace': RESP bulk
            # strings are binary-safe and may contain NUL bytes, so a bare
            # .decode() of the C string truncates at the first NUL and can
            # raise on invalid UTF-8. Matches cy_redis_client.pyx.
            if reply.str:
                return reply.str[:reply.len].decode('utf-8', errors='replace')
            return None
        elif reply.type == REDIS_REPLY_ARRAY:
            result = []
            for i in range(reply.elements):
                result.append(self._parse_reply(reply.element[i], depth + 1))
            return result
        elif reply.type == REDIS_REPLY_INTEGER:
            return reply.integer
        elif reply.type == REDIS_REPLY_NIL:
            return None
        elif reply.type == REDIS_REPLY_STATUS:
            if reply.str:
                return reply.str[:reply.len].decode('utf-8', errors='replace')
            return None
        elif reply.type == REDIS_REPLY_ERROR:
            if reply.str and reply.len > 0:
                raise RedisError(reply.str[:reply.len].decode('utf-8', errors='replace'))
            raise RedisError("Unknown error")
        else:
            return None

# Connection pool
cdef class ConnectionPool:
    cdef public list connections
    cdef readonly int max_connections
    cdef object lock
    cdef readonly str host
    cdef readonly int port
    cdef double timeout

    def __cinit__(self, str host="localhost", int port=6379, int max_connections=10, double timeout=5.0):
        # Preconditions: a pool needs a positive capacity and a valid TCP port;
        # these are caller errors caught loudly rather than failing later.
        assert max_connections > 0, "max_connections must be positive"
        assert 0 < port <= 65535, "port out of range"
        self.connections = []
        self.max_connections = max_connections
        self.lock = threading.Lock()
        self.host = host
        self.port = port
        self.timeout = timeout

    cpdef get_connection(self):
        """Get a connection from the pool"""
        # Invariant: the idle list never exceeds capacity.
        assert len(self.connections) <= self.max_connections, "idle pool overflow"
        with self.lock:
            if self.connections:
                return self.connections.pop()
            elif len(self.connections) < self.max_connections:
                conn = RedisConnection(self.host, self.port)
                conn._connect()
                return conn
        return None

    cpdef return_connection(self, conn):
        """Return connection to pool"""
        # Precondition: only RedisConnection instances belong in this pool.
        assert conn is not None, "cannot return a None connection"
        assert isinstance(conn, RedisConnection), "pool holds RedisConnection only"
        cdef RedisConnection c = conn
        with self.lock:
            if len(self.connections) < self.max_connections and c.state.state == CONN_CONNECTED:
                self.connections.append(c)
            else:
                c._disconnect()
            # Invariant: returning a connection never overruns capacity.
            assert len(self.connections) <= self.max_connections, "idle pool overflow"

# Main Redis client
cdef class CyRedisClient:
    cdef ConnectionPool pool
    cdef object executor
    cdef dict stream_offsets
    cdef object offset_lock

    def __cinit__(self, str host="localhost", int port=6379, int max_connections=10, int max_workers=4):
        self.pool = ConnectionPool(host, port, max_connections)
        self.executor = None  # We'll use C-level threading instead
        self.stream_offsets = {}
        self.offset_lock = threading.Lock()

    def __dealloc__(self):
        if self.pool:
            # Clean up connections in pool
            pass

    # Core Redis operations through hiredis
    def set(self, str key, str value, int ex=-1, int px=-1, bint nx=False, bint xx=False):
        """SET command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn is None:
            raise ConnectionError("No available connections")

        try:
            args = ['SET', key, value]
            if ex > 0:
                args.extend(['EX', str(ex)])
            elif px > 0:
                args.extend(['PX', str(px)])
            if nx:
                args.append('NX')
            elif xx:
                args.append('XX')
            return conn._execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def get(self, str key):
        """GET command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn is None:
            raise ConnectionError("No available connections")

        try:
            return conn._execute_command(['GET', key])
        finally:
            self.pool.return_connection(conn)

    def delete(self, str key):
        """DEL command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn is None:
            raise ConnectionError("No available connections")

        try:
            return conn._execute_command(['DEL', key])
        finally:
            self.pool.return_connection(conn)

    def publish(self, str channel, str message):
        """PUBLISH command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn is None:
            raise ConnectionError("No available connections")

        try:
            return conn._execute_command(['PUBLISH', channel, message])
        finally:
            self.pool.return_connection(conn)

    def xadd(self, str stream, dict data, str message_id='*'):
        """XADD command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn is None:
            raise ConnectionError("No available connections")

        try:
            args = ['XADD', stream, message_id]
            for k, v in data.items():
                args.extend([k, str(v)])
            return conn._execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def xread(self, dict streams, int count=10, int block=1000):
        """XREAD command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn is None:
            raise ConnectionError("No available connections")

        try:
            args = ['XREAD', 'COUNT', str(count), 'BLOCK', str(block), 'STREAMS']
            args.extend(streams.keys())
            args.extend(streams.values())
            result = conn._execute_command(args)
            if result is None:
                return []
            return self._parse_xread_result(result)
        finally:
            self.pool.return_connection(conn)

    cdef list _parse_xread_result(self, list result):
        """Parse XREAD result"""
        # Precondition: XREAD replies are an array (parsed to a list) of
        # per-stream entries; the caller already handled the nil (no data) case.
        assert result is not None, "xread result must not be None here"
        assert isinstance(result, list), "xread result must be a list"
        parsed = []
        for stream_data in result:
            if len(stream_data) >= 2:
                stream_name = stream_data[0]
                messages = stream_data[1]
                for msg in messages:
                    if len(msg) >= 2:
                        msg_id = msg[0]
                        msg_data = {}
                        # Parse field-value pairs
                        for i in range(1, len(msg), 2):
                            if i + 1 < len(msg):
                                field = msg[i]
                                value = msg[i + 1]
                                msg_data[field] = value
                        parsed.append((stream_name, msg_id, msg_data))
        return parsed

# Exception classes
class RedisError(Exception):
    pass

class ConnectionError(RedisError):
    pass
