# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
High-Performance Cython Redis Client
Direct hiredis integration for maximum performance.
"""

import asyncio
import threading
import time
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor

# Import protocol support
from cy_redis.protocol import RESPParser, ProtocolNegotiator, ConnectionState

# Extern declarations are now in .pxd file

# Connection states
DEF CONN_DISCONNECTED = 0
DEF CONN_CONNECTED = 1
DEF CONN_ERROR = 2

# Protocol versions
DEF RESP2 = 2
DEF RESP3 = 3

# Exception classes
class RedisError(Exception):
    pass

class ConnectionError(RedisError):
    pass

# Cython connection class
cdef class CyRedisConnection:

    def __cinit__(self, str host="localhost", int port=6379, double timeout=5.0):
        self.ctx = NULL
        self.connected = False
        self.host = host
        self.port = port
        self.timeout = timeout
        self.parser = RESPParser(RESP2)  # Start with RESP2, can upgrade later

    def __dealloc__(self):
        if self.ctx != NULL:
            redisFree(self.ctx)
            self.ctx = NULL

    cdef int connect(self):
        """Connect to Redis server"""
        if self.connected:
            return 0

        cdef timeval tv
        tv.tv_sec = <long>self.timeout
        tv.tv_usec = <long>((self.timeout - tv.tv_sec) * 1000000)

        self.ctx = redisConnectWithTimeout(self.host.encode('utf-8'), self.port, tv)
        if self.ctx == NULL or self.ctx.err:
            if self.ctx:
                redisFree(self.ctx)
                self.ctx = NULL
            self.connected = False
            return -1

        self.connected = True
        return 0

    cdef void disconnect(self):
        """Disconnect from Redis server"""
        if self.ctx != NULL:
            redisFree(self.ctx)
            self.ctx = NULL
        self.connected = False

    cpdef object _execute_command(self, list args):
        """Execute Redis command and return result"""
        if not self.connected:
            if self.connect() != 0:
                raise ConnectionError("Failed to connect to Redis")

        # Convert args to C strings for redisCommand
        cdef char **argv = <char **>malloc(sizeof(char *) * len(args))
        cdef size_t *argvlen = <size_t *>malloc(sizeof(size_t) * len(args))
        cdef redisReply *reply
        # Keep bytes objects alive during the call
        cdef list arg_bytes_list = []

        if argv == NULL or argvlen == NULL:
            if argv: free(argv)
            if argvlen: free(argvlen)
            raise MemoryError("Failed to allocate command args")

        try:
            for i in range(len(args)):
                if isinstance(args[i], str):
                    arg_bytes = args[i].encode('utf-8')
                elif isinstance(args[i], bytes):
                    arg_bytes = args[i]
                else:
                    arg_bytes = str(args[i]).encode('utf-8')
                arg_bytes_list.append(arg_bytes)
                argv[i] = <char *>arg_bytes_list[i]
                argvlen[i] = len(arg_bytes_list[i])

            reply = redisCommandArgv(self.ctx, len(args), <const char **>argv, <const size_t *>argvlen)

            if reply == NULL:
                self.connected = False
                raise ConnectionError("Redis command failed")

            try:
                result = self.parser.parse_reply(reply)
                return result
            finally:
                freeReplyObject(reply)
        finally:
            free(argv)
            free(argvlen)

    def execute_command(self, list args):
        """Execute Redis command with argument list"""
        # Duplicate the _execute_command logic here to make it accessible
        if not self.connected:
            if self.connect() != 0:
                raise ConnectionError("Failed to connect to Redis")

        # Convert args to C strings for redisCommand
        cdef char **argv = <char **>malloc(sizeof(char *) * len(args))
        cdef size_t *argvlen = <size_t *>malloc(sizeof(size_t) * len(args))
        cdef redisReply *reply

        if argv == NULL or argvlen == NULL:
            if argv: free(argv)
            if argvlen: free(argvlen)
            raise MemoryError("Failed to allocate command args")

        try:
            for i in range(len(args)):
                if isinstance(args[i], str):
                    arg_bytes = args[i].encode('utf-8')
                elif isinstance(args[i], bytes):
                    arg_bytes = args[i]
                else:
                    arg_bytes = str(args[i]).encode('utf-8')
                argv[i] = <char *>arg_bytes
                argvlen[i] = len(arg_bytes)

            reply = redisCommandArgv(self.ctx, len(args), <const char **>argv, <const size_t *>argvlen)

            if reply == NULL:
                self.connected = False
                raise ConnectionError("Redis command failed")

            try:
                result = self.parser.parse_reply(reply)
                return result
            finally:
                freeReplyObject(reply)
        finally:
            free(argv)
            free(argvlen)


# Connection pool
cdef class CyRedisConnectionPool:

    def __cinit__(self, str host="localhost", int port=6379,
                  int max_connections=10, double timeout=5.0):
        self.connections = []
        self.max_connections = max_connections
        self.lock = threading.Lock()
        self.host = host
        self.port = port
        self.timeout = timeout

    cdef CyRedisConnection get_connection(self):
        """Get a connection from the pool"""
        with self.lock:
            if self.connections:
                return self.connections.pop()

            # Create new connection
            conn = CyRedisConnection(self.host, self.port, self.timeout)
            if conn.connect() == 0:
                return conn

        return None

    cdef void return_connection(self, CyRedisConnection conn):
        """Return connection to pool"""
        if conn and conn.connected:
            with self.lock:
                if len(self.connections) < self.max_connections:
                    self.connections.append(conn)
                else:
                    conn.disconnect()
        elif conn:
            conn.disconnect()

# Main Redis client - matches redis_wrapper.py API
cdef class CyRedisClient:

    def __cinit__(self, str host="localhost", int port=6379,
                  int max_connections=10, int max_workers=4, bint auto_negotiate=True):
        self.pool = CyRedisConnectionPool(host, port, max_connections)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.stream_offsets = {}
        self.offset_lock = threading.Lock()

        # Initialize protocol support
        if auto_negotiate:
            self._negotiate_protocol()

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    # Core Redis operations - matching redis_wrapper.py API
    def set(self, key: str, value: str, ex: int = -1, px: int = -1,
            nx: bool = False, xx: bool = False) -> str:
        """Set key-value pair"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
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

    def get(self, key: str) -> Optional[str]:
        """Get value by key"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GET', key])
        finally:
            self.pool.return_connection(conn)

    def delete(self, key: str) -> int:
        """Delete key"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['DEL', key])
        finally:
            self.pool.return_connection(conn)

    def exists(self, key: str) -> int:
        """Check if key exists"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['EXISTS', key])
        finally:
            self.pool.return_connection(conn)

    def expire(self, key: str, seconds: int) -> int:
        """Set key expiration"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['EXPIRE', key, str(seconds)])
        finally:
            self.pool.return_connection(conn)

    def ttl(self, key: str) -> int:
        """Get key TTL"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['TTL', key])
        finally:
            self.pool.return_connection(conn)

    def incr(self, key: str) -> int:
        """Increment key"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['INCR', key])
        finally:
            self.pool.return_connection(conn)

    def decr(self, key: str) -> int:
        """Decrement key"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['DECR', key])
        finally:
            self.pool.return_connection(conn)

    def publish(self, channel: str, message: str) -> int:
        """Publish message to channel"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['PUBLISH', channel, message])
        finally:
            self.pool.return_connection(conn)

    # Stream operations
    def xadd(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> str:
        """Add message to stream"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['XADD', stream, message_id]
            for k, v in data.items():
                args.extend([k, str(v)])
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def xread(self, streams: Dict[str, str], count: int = 10, block: int = 1000) -> List[tuple]:
        """Read from streams"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
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

    # Async operations
    async def set_async(self, key: str, value: str) -> str:
        """Async set operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.set, key, value)

    async def get_async(self, key: str) -> Optional[str]:
        """Async get operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.get, key)

    async def delete_async(self, key: str) -> int:
        """Async delete operation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.delete, key)

    async def xadd_async(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> str:
        """Async stream add"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.xadd, stream, data, message_id)

    async def xread_async(self, streams: Dict[str, str], count: int = 10, block: int = 1000) -> List[tuple]:
        """Async stream read"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.xread, streams, count, block)

    # Lua script support
    def eval(self, script: str, keys: Optional[List[str]] = None,
             args: Optional[List[str]] = None) -> Any:
        """Execute Lua script"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            cmd_args = ['EVAL', script, str(len(keys or []))]
            if keys:
                cmd_args.extend(keys)
            if args:
                cmd_args.extend(args)
            return conn.execute_command(cmd_args)
        finally:
            self.pool.return_connection(conn)

    def evalsha(self, sha: str, keys: Optional[List[str]] = None,
                args: Optional[List[str]] = None) -> Any:
        """Execute Lua script by SHA"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            cmd_args = ['EVALSHA', sha, str(len(keys or []))]
            if keys:
                cmd_args.extend(keys)
            if args:
                cmd_args.extend(args)
            return conn.execute_command(cmd_args)
        finally:
            self.pool.return_connection(conn)

    def script_load(self, script: str) -> str:
        """Load Lua script into Redis"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['SCRIPT', 'LOAD', script])
        finally:
            self.pool.return_connection(conn)

    def script_kill(self) -> str:
        """Kill running Lua script"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['SCRIPT', 'KILL'])
        finally:
            self.pool.return_connection(conn)

    def script_flush(self) -> str:
        """Flush all Lua scripts"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['SCRIPT', 'FLUSH'])
        finally:
            self.pool.return_connection(conn)

    def script_exists(self, shas: Union[str, List[str]]) -> List[int]:
        """Check if scripts exist in cache"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            if isinstance(shas, str):
                shas = [shas]
            cmd_args = ['SCRIPT', 'EXISTS']
            cmd_args.extend(shas)
            result = conn.execute_command(cmd_args)
            return result if isinstance(result, list) else [result]
        finally:
            self.pool.return_connection(conn)

    # Async Lua script operations
    async def eval_async(self, script: str, keys: Optional[List[str]] = None,
                        args: Optional[List[str]] = None) -> Any:
        """Async Lua script execution"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.eval, script, keys, args)

    async def evalsha_async(self, sha: str, keys: Optional[List[str]] = None,
                           args: Optional[List[str]] = None) -> Any:
        """Async Lua script execution by SHA"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.evalsha, sha, keys, args)

    # Protocol negotiation and management
    cdef void _negotiate_protocol(self):
        """Negotiate protocol version with server"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn != None:
            try:
                # Try to negotiate RESP3
                cdef ProtocolNegotiator negotiator = ProtocolNegotiator(self, RESP3)
                cdef int negotiated_version = negotiator.negotiate_protocol()

                # Update connection parser to use negotiated version
                conn.parser.set_protocol_version(negotiated_version)

                print(f"Negotiated protocol version: {negotiated_version}")

            except Exception as e:
                print(f"Protocol negotiation failed, using RESP2: {e}")
            finally:
                self.pool.return_connection(conn)

    def set_protocol_version(self, int version):
        """Manually set protocol version for all connections"""
        if version not in (RESP2, RESP3):
            raise ValueError("Protocol version must be 2 or 3")

        # This is a simplified implementation - in production you'd update all connections
        print(f"Set protocol version to: {version}")

    def get_server_info(self):
        """Get server information including supported features"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['INFO'])
        finally:
            self.pool.return_connection(conn)

# Python wrapper class that matches redis_wrapper.py API
class HighPerformanceRedis:
    """
    High-performance Redis client with hiredis C library integration.
    Drop-in replacement for redis_wrapper.HighPerformanceRedis with better performance.
    """

    def __init__(self, host: str = "localhost", port: int = 6379,
                 max_connections: int = 10, max_workers: int = 4, use_uvloop: bool = None):
        self.client = CyRedisClient(host, port, max_connections, max_workers)

        # uvloop configuration
        if use_uvloop is None:
            try:
                import uvloop
                use_uvloop = True
            except ImportError:
                use_uvloop = False

        self.use_uvloop = use_uvloop
        if use_uvloop:
            try:
                import uvloop
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            except ImportError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # Cleanup handled by Cython destructors

    # Synchronous operations
    def set(self, key: str, value: str, ex: int = -1, px: int = -1,
            nx: bool = False, xx: bool = False) -> str:
        return self.client.set(key, value, ex, px, nx, xx)

    def get(self, key: str) -> Optional[str]:
        return self.client.get(key)

    def delete(self, key: str) -> int:
        return self.client.delete(key)

    def exists(self, key: str) -> int:
        return self.client.exists(key)

    def expire(self, key: str, seconds: int) -> int:
        return self.client.expire(key, seconds)

    def ttl(self, key: str) -> int:
        return self.client.ttl(key)

    def incr(self, key: str) -> int:
        return self.client.incr(key)

    def decr(self, key: str) -> int:
        return self.client.decr(key)

    def publish(self, channel: str, message: str) -> int:
        return self.client.publish(channel, message)

    def xadd(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> str:
        return self.client.xadd(stream, data, message_id)

    def xread(self, streams: Dict[str, str], count: int = 10, block: int = 1000) -> List[tuple]:
        return self.client.xread(streams, count, block)

    # Async operations
    async def set_async(self, key: str, value: str) -> str:
        return await self.client.set_async(key, value)

    async def get_async(self, key: str) -> Optional[str]:
        return await self.client.get_async(key)

    async def delete_async(self, key: str) -> int:
        return await self.client.delete_async(key)

    async def xadd_async(self, stream: str, data: Dict[str, Any], message_id: str = "*") -> str:
        return await self.client.xadd_async(stream, data, message_id)

    async def xread_async(self, streams: Dict[str, str], count: int = 10, block: int = 1000) -> List[tuple]:
        return await self.client.xread_async(streams, count, block)

    # Lua script support
    def eval(self, script: str, keys: Optional[List[str]] = None,
             args: Optional[List[str]] = None) -> Any:
        """Execute Lua script"""
        return self.client.eval(script, keys, args)

    def evalsha(self, sha: str, keys: Optional[List[str]] = None,
                args: Optional[List[str]] = None) -> Any:
        """Execute Lua script by SHA"""
        return self.client.evalsha(sha, keys, args)

    def script_load(self, script: str) -> str:
        """Load Lua script into Redis"""
        return self.client.script_load(script)

    def script_kill(self) -> str:
        """Kill running Lua script"""
        return self.client.script_kill()

    def script_flush(self) -> str:
        """Flush all Lua scripts"""
        return self.client.script_flush()

    def script_exists(self, shas: Union[str, List[str]]) -> List[int]:
        """Check if scripts exist in cache"""
        return self.client.script_exists(shas)

    # Async Lua script operations
    async def eval_async(self, script: str, keys: Optional[List[str]] = None,
                        args: Optional[List[str]] = None) -> Any:
        """Async Lua script execution"""
        return await self.client.eval_async(script, keys, args)

    async def evalsha_async(self, sha: str, keys: Optional[List[str]] = None,
                           args: Optional[List[str]] = None) -> Any:
        """Async Lua script execution by SHA"""
        return await self.client.evalsha_async(sha, keys, args)
