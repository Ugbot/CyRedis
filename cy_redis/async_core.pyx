# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
Async Redis Core - High-performance Cython implementation with true non-GIL threading
Implements Redis protocol parsing, async state machines, and message processing in C.
"""

import asyncio
import threading
import time
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memset, strlen, strcpy, strdup
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int32_t, int64_t
from libc.errno cimport errno
from cpython.ref cimport Py_INCREF, Py_DECREF, PyObject
from cpython.exc cimport PyErr_CheckSignals
from concurrent.futures import ThreadPoolExecutor
import socket
import select

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

# Hiredis includes for protocol parsing
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
    redisContext *redisConnectWithTimeout(const char *ip, int port, const struct timeval tv)
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
    struct timeval:
        long tv_sec
        long tv_usec

# AsyncIO integration
cdef extern from "Python.h":
    object PyMemoryView_FromMemory(char *mem, Py_ssize_t size, int flags)
    int PyBUF_READ
    int PyBUF_WRITE

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

# C-level structures for zero-copy operations
cdef struct AsyncMessage:
    int msg_type
    void *data
    size_t data_len
    uint64_t timestamp
    void *callback
    void *user_data

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

cdef struct MessageQueue:
    AsyncMessage *messages
    size_t capacity
    size_t head
    size_t tail
    size_t count
    pthread_mutex_t lock
    pthread_cond_t not_empty
    pthread_cond_t not_full

# Thread-safe message queue implementation
cdef class AsyncMessageQueue:
    cdef MessageQueue *queue

    def __cinit__(self, size_t capacity=1024):
        self.queue = <MessageQueue*>malloc(sizeof(MessageQueue))
        if self.queue == NULL:
            raise MemoryError("Failed to allocate message queue")

        memset(self.queue, 0, sizeof(MessageQueue))
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

# Async Redis Connection with state machine
cdef class AsyncRedisConnection:
    cdef ConnectionState *state
    cdef AsyncMessageQueue *command_queue
    cdef AsyncMessageQueue *response_queue
    cdef object loop
    cdef object thread
    cdef bint running

    def __cinit__(self, str host="localhost", int port=6379):
        # Initialize connection state
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

        # Initialize message queues
        self.command_queue = new AsyncMessageQueue(1024)
        self.response_queue = new AsyncMessageQueue(1024)

        self.running = False
        self.loop = None
        self.thread = None

    def __dealloc__(self):
        if self.state != NULL:
            if self.state.ctx != NULL:
                redisFree(self.state.ctx)
            if self.state.host != NULL:
                free(self.state.host)
            if self.state.buffer != NULL:
                free(self.state.buffer)
            free(self.state)

        if self.command_queue != NULL:
            del self.command_queue
        if self.response_queue != NULL:
            del self.response_queue

    cdef int connect(self) nogil:
        """Establish connection to Redis server"""
        cdef timeval tv
        tv.tv_sec = CONNECTION_TIMEOUT / 1000
        tv.tv_usec = (CONNECTION_TIMEOUT % 1000) * 1000

        self.state.ctx = redisConnectWithTimeout(self.state.host, self.state.port, tv)
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

    cdef int send_command(self, const char *command, size_t cmd_len) nogil:
        """Send command to Redis server"""
        if self.state.state != CONN_CONNECTED:
            return -1

        # For now, use hiredis redisCommand
        # TODO: Implement async command sending
        cdef redisReply *reply = redisCommand(self.state.ctx, command)
        if reply == NULL:
            self.state.state = CONN_ERROR
            return -1

        freeReplyObject(reply)
        self.state.last_activity = <uint64_t>(time.time() * 1000000)
        return 0

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

    def start_async_loop(self):
        """Start the async event loop in a separate thread"""
        if self.running:
            return

        self.running = True
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self.thread.start()

    def stop_async_loop(self):
        """Stop the async event loop"""
        if not self.running:
            return

        self.running = False
        if self.loop and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.loop.stop)

        if self.thread:
            self.thread.join(timeout=1.0)

    def _run_async_loop(self):
        """Run the async event loop"""
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    async def execute_command_async(self, str command):
        """Execute Redis command asynchronously"""
        # Create future for result
        future = self.loop.create_future()

        # Queue command for processing
        cdef AsyncMessage msg
        msg.msg_type = MSG_COMMAND
        msg.data = <void*>strdup(command.encode('utf-8'))
        msg.data_len = len(command.encode('utf-8'))
        msg.timestamp = <uint64_t>(time.time() * 1000000)
        msg.callback = <void*>future
        msg.user_data = NULL

        # Enqueue message (this will be handled by the C thread)
        with nogil:
            self.command_queue.enqueue(&msg)

        # Wait for result
        return await future

# Async Redis Client with true non-GIL threading
cdef class AsyncRedisClient:
    cdef:
        AsyncRedisConnection **connections
        size_t num_connections
        size_t max_connections
        AsyncMessageQueue *work_queue
        threading.Thread **worker_threads
        size_t num_worker_threads
        bint running

    def __cinit__(self, str host="localhost", int port=6379, size_t max_connections=10, size_t num_workers=4):
        self.max_connections = max_connections
        self.num_connections = 0
        self.connections = <AsyncRedisConnection**>malloc(sizeof(AsyncRedisConnection*) * max_connections)
        if self.connections == NULL:
            raise MemoryError("Failed to allocate connections array")

        memset(self.connections, 0, sizeof(AsyncRedisConnection*) * max_connections)

        # Initialize work queue
        self.work_queue = new AsyncMessageQueue(4096)

        # Initialize worker threads array
        self.num_worker_threads = num_workers
        self.worker_threads = <void**>malloc(sizeof(void*) * num_workers)
        if self.worker_threads == NULL:
            del self.work_queue
            free(self.connections)
            raise MemoryError("Failed to allocate worker threads array")

        memset(self.worker_threads, 0, sizeof(void*) * num_workers)
        self.running = False

    def __dealloc__(self):
        if self.connections != NULL:
            for i in range(self.num_connections):
                if self.connections[i] != NULL:
                    del self.connections[i]
            free(self.connections)

        if self.work_queue != NULL:
            del self.work_queue

        if self.worker_threads != NULL:
            free(self.worker_threads)

    cdef AsyncRedisConnection* get_connection(self):
        """Get an available connection"""
        # Simple round-robin for now
        if self.num_connections == 0:
            return NULL

        cdef size_t index = 0  # TODO: Implement proper load balancing
        return self.connections[index]

    def start_workers(self):
        """Start worker threads for processing commands"""
        if self.running:
            return

        self.running = True

        for i in range(self.num_worker_threads):
            self.worker_threads[i] = new threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            self.worker_threads[i].start()

    def stop_workers(self):
        """Stop all worker threads"""
        if not self.running:
            return

        self.running = False

        # Send stop messages to all workers
        cdef AsyncMessage stop_msg
        stop_msg.msg_type = MSG_CLOSE
        stop_msg.data = NULL
        stop_msg.data_len = 0
        stop_msg.timestamp = <uint64_t>(time.time() * 1000000)
        stop_msg.callback = NULL
        stop_msg.user_data = NULL

        for i in range(self.num_worker_threads):
            with nogil:
                self.work_queue.enqueue(&stop_msg)

        # Wait for threads to finish
        for i in range(self.num_worker_threads):
            if self.worker_threads[i] != NULL:
                self.worker_threads[i].join(timeout=1.0)

    def _worker_loop(self, int worker_id):
        """Worker thread main loop - runs without GIL"""
        cdef AsyncMessage msg

        while self.running:
            # Get next message from queue (with nogil for true parallelism)
            with nogil:
                if self.work_queue.dequeue(&msg) != 0:
                    continue

            if msg.msg_type == MSG_CLOSE:
                break
            elif msg.msg_type == MSG_COMMAND:
                # Process command
                self._process_command(&msg)
            else:
                # Unknown message type
                pass

    cdef void _process_command(self, AsyncMessage *msg) nogil:
        """Process a command message"""
        cdef AsyncRedisConnection *conn = self.get_connection()
        if conn == NULL:
            # No available connection, send error response
            return

        # Execute command
        cdef int result = conn.send_command(<const char*>msg.data, msg.data_len)
        if result != 0:
            # Command failed, send error response
            return

        # Command succeeded, send success response
        # TODO: Implement response handling

    async def execute_async(self, str command):
        """Execute Redis command asynchronously with true parallelism"""
        if not self.running:
            self.start_workers()

        # Create future for result
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        # Queue command for processing by worker threads
        cdef AsyncMessage msg
        msg.msg_type = MSG_COMMAND
        msg.data = <void*>strdup(command.encode('utf-8'))
        msg.data_len = len(command.encode('utf-8'))
        msg.timestamp = <uint64_t>(time.time() * 1000000)
        msg.callback = <void*>future
        msg.user_data = NULL

        # Enqueue message (this blocks if queue is full)
        with nogil:
            self.work_queue.enqueue(&msg)

        # Wait for result
        return await future

# Exception classes
class RedisError(Exception):
    pass

class ConnectionError(RedisError):
    pass

# Python wrapper for async operations
class AsyncRedisWrapper:
    """Python wrapper for the high-performance async Redis client"""

    def __init__(self, host="localhost", port=6379, max_connections=10, num_workers=4):
        self.client = AsyncRedisClient(host, port, max_connections, num_workers)
        self.client.start_workers()

    def __del__(self):
        if hasattr(self, 'client'):
            self.client.stop_workers()

    async def set(self, key: str, value: str) -> str:
        """Set a key-value pair asynchronously"""
        command = f"SET {key} {value}"
        return await self.client.execute_async(command)

    async def get(self, key: str) -> str:
        """Get the value of a key asynchronously"""
        command = f"GET {key}"
        return await self.client.execute_async(command)

    async def delete(self, key: str) -> int:
        """Delete a key asynchronously"""
        command = f"DEL {key}"
        return await self.client.execute_async(command)
