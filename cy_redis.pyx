# cython: language_level=3
# distutils: language=c

__version__ = "0.1.0"

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any, Union, Callable
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen
from cpython.ref cimport Py_INCREF, Py_DECREF
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

# Thread-safe connection pool
cdef class ConnectionPool:
    cdef:
        list connections
        int max_connections
        object lock
        str host
        int port
        double timeout

    def __cinit__(self, str host="localhost", int port=6379, int max_connections=10, double timeout=5.0):
        self.connections = []
        self.max_connections = max_connections
        self.lock = threading.Lock()
        self.host = host
        self.port = port
        self.timeout = timeout

    cdef redisContext* _get_connection(self):
        with self.lock:
            if self.connections:
                return self.connections.pop()
            elif len(self.connections) < self.max_connections:
                cdef redisContext* ctx
                cdef timeval tv
                tv.tv_sec = <long>self.timeout
                tv.tv_usec = <long>((self.timeout - tv.tv_sec) * 1000000)
                ctx = redisConnectWithTimeout(self.host.encode('utf-8'), self.port, tv)
                if ctx == NULL or ctx.err:
                    if ctx:
                        redisFree(ctx)
                    raise ConnectionError(f"Failed to connect to Redis: {ctx.errstr.decode('utf-8') if ctx else 'Unknown error'}")
                return ctx
        return NULL

    cdef void _return_connection(self, redisContext* ctx):
        with self.lock:
            if len(self.connections) < self.max_connections:
                self.connections.append(ctx)
            else:
                redisFree(ctx)

    def close(self):
        with self.lock:
            for ctx in self.connections:
                if ctx:
                    redisFree(ctx)
            self.connections.clear()

# High-performance Redis client
cdef class CyRedisClient:
    cdef:
        ConnectionPool pool
        object executor
        dict stream_offsets
        object offset_lock

    def __cinit__(self, str host="localhost", int port=6379, int max_connections=10, int max_workers=4):
        self.pool = ConnectionPool(host, port, max_connections)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.stream_offsets = {}
        self.offset_lock = threading.Lock()

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)
        if self.pool:
            self.pool.close()

    cdef object _execute_command(self, list args):
        cdef redisContext* ctx = self.pool._get_connection()
        if ctx == NULL:
            raise ConnectionError("No available connections")

        try:
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
            cdef redisReply* reply = redisCommand(ctx, cmd_bytes)
            if reply == NULL:
                raise ConnectionError(f"Redis command failed: {ctx.errstr.decode('utf-8')}")

            try:
                result = self._parse_reply(reply)
                return result
            finally:
                freeReplyObject(reply)
        finally:
            self.pool._return_connection(ctx)

    cdef object _parse_reply(self, redisReply* reply):
        if reply.type == 1:  # REDIS_REPLY_STRING
            return reply.str.decode('utf-8') if reply.str else None
        elif reply.type == 2:  # REDIS_REPLY_ARRAY
            return [self._parse_reply(reply.element[i]) for i in range(reply.elements)]
        elif reply.type == 3:  # REDIS_REPLY_INTEGER
            return reply.integer
        elif reply.type == 4:  # REDIS_REPLY_NIL
            return None
        elif reply.type == 5:  # REDIS_REPLY_STATUS
            return reply.str.decode('utf-8') if reply.str else None
        elif reply.type == 6:  # REDIS_REPLY_ERROR
            raise RedisError(reply.str.decode('utf-8') if reply.str else "Unknown error")
        else:
            return None

    # High-level API methods
    def set(self, str key, str value):
        return self._execute_command(['SET', key, value])

    def get(self, str key):
        return self._execute_command(['GET', key])

    def delete(self, str key):
        return self._execute_command(['DEL', key])

    def publish(self, str channel, str message):
        return self._execute_command(['PUBLISH', channel, message])

    def xadd(self, str stream, dict data, str message_id='*'):
        args = ['XADD', stream, message_id]
        for k, v in data.items():
            args.extend([k, str(v)])
        return self._execute_command(args)

    def xread(self, dict streams, int count=10, int block=1000):
        args = ['XREAD', 'COUNT', str(count), 'BLOCK', str(block), 'STREAMS']
        args.extend(streams.keys())
        args.extend(streams.values())
        result = self._execute_command(args)
        if result is None:
            return []
        return self._parse_xread_result(result)

    cdef list _parse_xread_result(self, list result):
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

    # Threading support
    async def execute_async(self, str operation, *args):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, getattr(self, operation), *args)

    def execute_threaded(self, str operation, *args):
        future = self.executor.submit(getattr(self, operation), *args)
        return future.result()

# Stream manager with threading support
class ThreadedStreamManager:
    def __init__(self, redis_client: CyRedisClient, max_workers: int = 4):
        self.redis = redis_client
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.stream_offsets = {}
        self.offset_lock = threading.Lock()
        self.running = False
        self.threads = []

    def subscribe_to_streams(self, streams: Dict[str, str]):
        """Subscribe to multiple streams with threading"""
        with self.offset_lock:
            self.stream_offsets.update(streams)

    def start_consuming(self, callback: Callable[[str, str, Dict], None]):
        """Start consuming from streams in separate threads"""
        self.running = True
        for stream_name in self.stream_offsets.keys():
            thread = threading.Thread(
                target=self._consume_stream,
                args=(stream_name, callback),
                daemon=True
            )
            thread.start()
            self.threads.append(thread)

    def _consume_stream(self, stream_name: str, callback: Callable):
        """Consume messages from a single stream"""
        while self.running:
            try:
                with self.offset_lock:
                    offset = self.stream_offsets.get(stream_name, '0')

                messages = self.redis.xread({stream_name: offset}, count=1, block=1000)
                for stream, msg_id, data in messages:
                    callback(stream, msg_id, data)
                    with self.offset_lock:
                        self.stream_offsets[stream] = msg_id
            except Exception as e:
                print(f"Error consuming from {stream_name}: {e}")
                time.sleep(0.1)  # Brief pause on error

    def stop_consuming(self):
        """Stop all consumer threads"""
        self.running = False
        for thread in self.threads:
            thread.join(timeout=1.0)
        self.threads.clear()

# Exception classes
class RedisError(Exception):
    pass

class ConnectionError(RedisError):
    pass
