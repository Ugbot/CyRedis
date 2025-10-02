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
import threading
import time
import json
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, strlen, strcpy, strdup
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int32_t, int64_t
from libc.errno cimport errno
from cpython.ref cimport Py_INCREF, Py_DECREF, PyObject
from cpython.exc cimport PyErr_CheckSignals

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

    redisContext *redisConnect(const char *ip, int port)
    redisContext *redisConnectWithTimeout(const char *ip, int port, const timeval *tv)
    redisContext *redisConnectUnix(const char *path)
    void redisFree(redisContext *c)
    int redisReconnect(redisContext *c)
    redisReply *redisCommand(redisContext *c, const char *format, ...)
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

# Redis connection with hiredis
cdef class RedisConnection:
    cdef ConnectionState *state

    def __cinit__(self, str host="localhost", int port=6379):
        self.state = <ConnectionState*>malloc(sizeof(ConnectionState))
        if self.state == NULL:
            raise MemoryError("Failed to allocate connection state")

        memset(self.state, 0, sizeof(ConnectionState))
        self.state.state = CONN_DISCONNECTED
        self.state.host = strdup(host.encode('utf-8'))
        self.state.port = port
        self.state.buffer_size = BUFFER_SIZE
        self.state.buffer = <char*>malloc(BUFFER_SIZE)
        if self.state.buffer == NULL:
            free(self.state)
            raise MemoryError("Failed to allocate buffer")

    def __dealloc__(self):
        if self.state != NULL:
            if self.state.ctx != NULL:
                redisFree(self.state.ctx)
            if self.state.host != NULL:
                free(self.state.host)
            if self.state.buffer != NULL:
                free(self.state.buffer)
            free(self.state)

    cdef int connect(self) nogil:
        """Establish connection to Redis server using hiredis"""
        cdef timeval tv
        tv.tv_sec = CONNECTION_TIMEOUT / 1000
        tv.tv_usec = (CONNECTION_TIMEOUT % 1000) * 1000

        self.state.ctx = redisConnectWithTimeout(self.state.host, self.state.port, &tv)
        if self.state.ctx == NULL or self.state.ctx.err:
            self.state.state = CONN_ERROR
            return -1

        self.state.state = CONN_CONNECTED
        self.state.fd = self.state.ctx.fd
        self.state.last_activity = <uint64_t>(time.time() * 1000000)
        return 0

    cdef int disconnect(self) nogil:
        """Disconnect from Redis server"""
        if self.state.ctx != NULL:
            redisFree(self.state.ctx)
            self.state.ctx = NULL

        self.state.state = CONN_DISCONNECTED
        return 0

    cdef object execute_command(self, list args):
        """Execute Redis command using hiredis"""
        if self.state.state != CONN_CONNECTED:
            if self.connect() != 0:
                raise ConnectionError("Failed to connect to Redis")

        # Build command string
        cdef str cmd = "*%d\r\n" % len(args)
        for arg in args:
            if isinstance(arg, str):
                arg_bytes = arg.encode('utf-8')
            elif isinstance(arg, bytes):
                arg_bytes = arg
            else:
                arg_bytes = str(arg).encode('utf-8')
            cmd += "$%d\r\n%s\r\n" % (len(arg_bytes), arg_bytes.decode('utf-8'))

        cdef bytes cmd_bytes = cmd.encode('utf-8')
        cdef redisReply *reply = redisCommand(self.state.ctx, cmd_bytes)
        if reply == NULL:
            raise ConnectionError("Redis command failed")

        try:
            result = self._parse_reply(reply)
            return result
        finally:
            freeReplyObject(reply)
            self.state.last_activity = <uint64_t>(time.time() * 1000000)

    cdef object _parse_reply(self, redisReply *reply):
        """Parse Redis reply into Python object"""
        if reply.type == REDIS_REPLY_STRING:
            return reply.str.decode('utf-8') if reply.str else None
        elif reply.type == REDIS_REPLY_ARRAY:
            return [self._parse_reply(reply.element[i]) for i in range(reply.elements)]
        elif reply.type == REDIS_REPLY_INTEGER:
            return reply.integer
        elif reply.type == REDIS_REPLY_NIL:
            return None
        elif reply.type == REDIS_REPLY_STATUS:
            return reply.str.decode('utf-8') if reply.str else None
        elif reply.type == REDIS_REPLY_ERROR:
            raise RedisError(reply.str.decode('utf-8') if reply.str else "Unknown error")
        else:
            return None

# Connection pool
cdef class ConnectionPool:
    cdef list connections
    cdef int max_connections
    cdef object lock
    cdef str host
    cdef int port
    cdef double timeout

    def __cinit__(self, str host="localhost", int port=6379, int max_connections=10, double timeout=5.0):
        self.connections = []
        self.max_connections = max_connections
        self.lock = threading.Lock()
        self.host = host
        self.port = port
        self.timeout = timeout

    cdef RedisConnection get_connection(self):
        """Get a connection from the pool"""
        with self.lock:
            if self.connections:
                return self.connections.pop()
            elif len(self.connections) < self.max_connections:
                conn = RedisConnection(self.host, self.port)
                conn.connect()
                return conn
        return None

    cdef void return_connection(self, RedisConnection conn):
        """Return connection to pool"""
        with self.lock:
            if len(self.connections) < self.max_connections and conn.state.state == CONN_CONNECTED:
                self.connections.append(conn)
            else:
                conn.disconnect()

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
        if conn == NULL:
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
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def get(self, str key):
        """GET command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn == NULL:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GET', key])
        finally:
            self.pool.return_connection(conn)

    def delete(self, str key):
        """DEL command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn == NULL:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['DEL', key])
        finally:
            self.pool.return_connection(conn)

    def publish(self, str channel, str message):
        """PUBLISH command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn == NULL:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['PUBLISH', channel, message])
        finally:
            self.pool.return_connection(conn)

    def xadd(self, str stream, dict data, str message_id='*'):
        """XADD command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn == NULL:
            raise ConnectionError("No available connections")

        try:
            args = ['XADD', stream, message_id]
            for k, v in data.items():
                args.extend([k, str(v)])
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def xread(self, dict streams, int count=10, int block=1000):
        """XREAD command through hiredis"""
        cdef RedisConnection conn = self.pool.get_connection()
        if conn == NULL:
            raise ConnectionError("No available connections")

        try:
            args = ['XREAD', 'COUNT', str(count), 'BLOCK', str(block), 'STREAMS']
            args.extend(streams.keys())
            args.extend(streams.values())
            result = conn.execute_command(args)
            if result is None:
                return []
            return self._parse_xread_result(result)
        finally:
            self.pool.return_connection(conn)

    cdef list _parse_xread_result(self, list result):
        """Parse XREAD result"""
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
