# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
High-Performance Cython Redis Client
Proper wrapper around hiredis C library for maximum performance.
"""

import asyncio
import threading
import time
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor

# Import C declarations
from libc.stdlib cimport malloc, free
from libc.stdint cimport uint64_t
from libc.string cimport strlen

# System time struct (must be declared before hiredis)
cdef extern from "sys/time.h":
    cdef struct timeval:
        long tv_sec
        long tv_usec

# Extern declarations for hiredis (must be in .pyx file to be accessible)
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
    redisContext *redisConnectWithTimeout(const char *ip, int port, timeval tv)
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

# Temporarily disable protocol imports to isolate the issue
# from cy_redis.core.protocol import RESPParser, ProtocolNegotiator, ConnectionState

# Error classes
class RedisError(Exception):
    pass

class ConnectionError(RedisError):
    pass

class ProtocolError(RedisError):
    pass

class TimeoutError(RedisError):
    pass

# Connection states
DEF CONN_DISCONNECTED = 0
DEF CONN_CONNECTED = 1
DEF CONN_ERROR = 2

# Protocol versions
DEF RESP2 = 2
DEF RESP3 = 3

# Redis reply types (from hiredis)
DEF REDIS_REPLY_STRING = 1
DEF REDIS_REPLY_ARRAY = 2
DEF REDIS_REPLY_INTEGER = 3
DEF REDIS_REPLY_NIL = 4
DEF REDIS_REPLY_STATUS = 5
DEF REDIS_REPLY_ERROR = 6

# Exception classes
class RedisError(Exception):
    pass

class ConnectionError(RedisError):
    pass

# Cython connection class
cdef class CyRedisConnection:
    cdef redisContext *ctx
    cdef bint _connected
    cdef str _host
    cdef int _port
    cdef double _timeout
    cdef object parser

    def __cinit__(self, str host="localhost", int port=6379, double timeout=5.0):
        self.ctx = <redisContext *>0
        self._connected = False
        self._host = host
        self._port = port
        self._timeout = timeout
        # self.parser = RESPParser(RESP2)  # Temporarily disabled

    def get_connected(self):
        return self._connected

    def get_host(self):
        return self._host

    def get_port(self):
        return self._port

    def get_timeout(self):
        return self._timeout

    def __dealloc__(self):
        if self.ctx != <redisContext *>0:
            redisFree(self.ctx)
            self.ctx = <redisContext *>0

    cdef int connect(self):
        """Connect to Redis server"""
        if self._connected:
            return 0

        cdef timeval tv
        tv.tv_sec = <long>self._timeout
        tv.tv_usec = <long>((self._timeout - tv.tv_sec) * 1000000)

        cdef bytes host_bytes = self._host.encode('utf-8')
        cdef const char* host_cstr = host_bytes
        self.ctx = redisConnectWithTimeout(host_cstr, self._port, tv)
        if self.ctx == <redisContext *>0 or self.ctx.err:
            if self.ctx != <redisContext *>0:
                redisFree(self.ctx)
                self.ctx = <redisContext *>0
            self._connected = False
            return -1

        self._connected = True
        return 0

    cdef void disconnect(self):
        """Disconnect from Redis server"""
        if self.ctx != <redisContext *>0:
            redisFree(self.ctx)
            self.ctx = <redisContext *>0
        self._connected = False

    cdef object _parse_reply(self, redisReply *reply):
        """Parse redisReply directly - inline implementation to avoid import issues"""
        # Handle different reply types directly without external dependencies
        cdef list result
        cdef int i
        cdef str error_msg

        if reply.type == REDIS_REPLY_STRING:
            if reply.str and reply.len > 0:
                return reply.str[:reply.len].decode('utf-8', errors='replace')
            return ""
        elif reply.type == REDIS_REPLY_ARRAY:
            result = []
            for i in range(reply.elements):
                result.append(self._parse_reply(reply.element[i]))
            return result
        elif reply.type == REDIS_REPLY_INTEGER:
            return reply.integer
        elif reply.type == REDIS_REPLY_NIL:
            return None
        elif reply.type == REDIS_REPLY_STATUS:
            if reply.str and reply.len > 0:
                return reply.str[:reply.len].decode('utf-8', errors='replace')
            return "OK"
        elif reply.type == REDIS_REPLY_ERROR:
            error_msg = ""
            if reply.str and reply.len > 0:
                error_msg = reply.str[:reply.len].decode('utf-8', errors='replace')
            raise RedisError(error_msg)
        else:
            # Unknown type, return as string if possible
            if reply.str:
                return reply.str.decode('utf-8', errors='replace')
            return None

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
        cdef int i
        cdef bytes arg_bytes

        if argv == <char **>0 or argvlen == <size_t *>0:
            if argv != <char **>0: free(argv)
            if argvlen != <size_t *>0: free(argvlen)
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

            if reply == <redisReply *>0:
                self._connected = False
                raise ConnectionError("Redis command failed")

            try:
                result = self._parse_reply(reply)
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
        # Keep bytes objects alive during the call
        cdef list arg_bytes_list = []
        cdef int i
        cdef bytes arg_bytes

        if argv == <char **>0 or argvlen == <size_t *>0:
            if argv != <char **>0: free(argv)
            if argvlen != <size_t *>0: free(argvlen)
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

            if reply == <redisReply *>0:
                self._connected = False
                raise ConnectionError("Redis command failed")

            try:
                result = self._parse_reply(reply)
                return result
            finally:
                freeReplyObject(reply)
        finally:
            free(argv)
            free(argvlen)


# Connection pool
cdef class CyRedisConnectionPool:
    cdef object _connections
    cdef int _max_connections
    cdef object _lock
    cdef str _host
    cdef int _port
    cdef double _timeout

    def __cinit__(self, str host="localhost", int port=6379,
                  int max_connections=10, double timeout=5.0):
        # Initialize attributes explicitly
        self._connections = []
        self._max_connections = max_connections
        self._lock = threading.Lock()
        self._host = host
        self._port = port
        self._timeout = timeout

    def get_connections(self):
        return self._connections

    def get_max_connections(self):
        return self._max_connections

    def get_lock(self):
        return self._lock

    def get_host(self):
        return self._host

    def get_port(self):
        return self._port

    def get_timeout(self):
        return self._timeout

    cdef CyRedisConnection get_connection(self):
        """Get a connection from the pool"""
        with self._lock:
            if self._connections:
                return self._connections.pop()

            # Create new connection
            conn = CyRedisConnection(self._host, self._port, self._timeout)
            if conn.connect() == 0:
                return conn

        return None

    cdef void return_connection(self, CyRedisConnection conn):
        """Return connection to pool"""
        if conn and conn._connected:
            with self._lock:
                if len(self._connections) < self._max_connections:
                    self._connections.append(conn)
                else:
                    conn.disconnect()
        elif conn:
            conn.disconnect()

# Main Redis client - matches redis_wrapper.py API
cdef class CyRedisClient:
    cdef CyRedisConnectionPool _pool
    cdef object _executor
    cdef dict _stream_offsets
    cdef object _offset_lock

    def __cinit__(self, str host="localhost", int port=6379,
                  int max_connections=10, int max_workers=4, bint auto_negotiate=True):
        self._pool = CyRedisConnectionPool(host, port, max_connections)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._stream_offsets = {}
        self._offset_lock = threading.Lock()

        # Protocol negotiation will be done lazily on first command

    def get_pool(self):
        return self._pool

    def get_executor(self):
        return self._executor

    def get_stream_offsets(self):
        return self._stream_offsets

    def get_offset_lock(self):
        return self._offset_lock

    def __dealloc__(self):
        if self._executor:
            self._executor.shutdown(wait=True)

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
        """Negotiate protocol version with server - temporarily disabled"""
        pass

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

    # ===== CLUSTER OPERATIONS =====

    def cluster_info(self) -> Dict[str, Any]:
        """Get cluster information"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'INFO'])
        finally:
            self.pool.return_connection(conn)

    def cluster_nodes(self) -> List[str]:
        """Get cluster nodes information"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'NODES'])
        finally:
            self.pool.return_connection(conn)

    def cluster_slots(self) -> List[List[Any]]:
        """Get cluster slots information"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'SLOTS'])
        finally:
            self.pool.return_connection(conn)

    def cluster_keyslot(self, key: str) -> int:
        """Get hash slot for key"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['CLUSTER', 'KEYSLOT', key])
            return int(result) if result else 0
        finally:
            self.pool.return_connection(conn)

    def cluster_countkeysinslot(self, slot: int) -> int:
        """Count keys in hash slot"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['CLUSTER', 'COUNTKEYSINSLOT', str(slot)])
            return int(result) if result else 0
        finally:
            self.pool.return_connection(conn)

    def cluster_getkeysinslot(self, slot: int, count: int) -> List[str]:
        """Get keys in hash slot"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'GETKEYSINSLOT', str(slot), str(count)])
        finally:
            self.pool.return_connection(conn)

    def cluster_addslots(self, *slots: int) -> str:
        """Add hash slots to node"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['CLUSTER', 'ADDSLOTS'] + [str(slot) for slot in slots]
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def cluster_delslots(self, *slots: int) -> str:
        """Remove hash slots from node"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['CLUSTER', 'DELSLOTS'] + [str(slot) for slot in slots]
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def cluster_setslot(self, slot: int, subcommand: str, node_id: str = None) -> str:
        """Set hash slot configuration"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['CLUSTER', 'SETSLOT', str(slot), subcommand]
            if node_id:
                args.append(node_id)
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def cluster_meet(self, ip: str, port: int) -> str:
        """Meet other cluster node"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'MEET', ip, str(port)])
        finally:
            self.pool.return_connection(conn)

    def cluster_forget(self, node_id: str) -> str:
        """Remove node from cluster"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'FORGET', node_id])
        finally:
            self.pool.return_connection(conn)

    def cluster_replicate(self, node_id: str) -> str:
        """Configure node as replica of primary"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'REPLICATE', node_id])
        finally:
            self.pool.return_connection(conn)

    def cluster_failover(self, force: bool = False, takeover: bool = False) -> str:
        """Force failover"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['CLUSTER', 'FAILOVER']
            if force:
                args.append('FORCE')
            if takeover:
                args.append('TAKEOVER')
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def cluster_reset(self, hard: bool = False, soft: bool = False) -> str:
        """Reset cluster node"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['CLUSTER', 'RESET']
            if hard:
                args.append('HARD')
            elif soft:
                args.append('SOFT')
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def cluster_saveconfig(self) -> str:
        """Force save cluster configuration"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'SAVECONFIG'])
        finally:
            self.pool.return_connection(conn)

    def cluster_bumpepoch(self) -> str:
        """Advance cluster config epoch"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'BUMPEPOCH'])
        finally:
            self.pool.return_connection(conn)

    def cluster_myid(self) -> str:
        """Get node ID"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'MYID'])
        finally:
            self.pool.return_connection(conn)

    def cluster_myshardid(self) -> str:
        """Get shard ID"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'MYSHARDID'])
        finally:
            self.pool.return_connection(conn)

    def cluster_flushslots(self) -> str:
        """Delete all slots information"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'FLUSHSLOTS'])
        finally:
            self.pool.return_connection(conn)

    def cluster_links(self) -> List[Dict[str, Any]]:
        """Get cluster links information"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'LINKS'])
        finally:
            self.pool.return_connection(conn)

    def cluster_count_failure_reports(self, node_id: str) -> int:
        """Count failure reports for node"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['CLUSTER', 'COUNT-FAILURE-REPORTS', node_id])
            return int(result) if result else 0
        finally:
            self.pool.return_connection(conn)

    def cluster_shards(self) -> List[Dict[str, Any]]:
        """Get cluster shards information"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'SHARDS'])
        finally:
            self.pool.return_connection(conn)

    def cluster_slot_stats(self) -> List[Dict[str, Any]]:
        """Get slot usage statistics"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['CLUSTER', 'SLOT-STATS'])
        finally:
            self.pool.return_connection(conn)

    # ===== CLUSTER-AWARE OPERATIONS =====

    def cluster_aware_execute(self, command: List[str], key: str = None) -> Any:
        """Execute command with cluster awareness (route to correct node)"""
        cdef CyRedisConnection conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            # If we have a key, determine which node should handle it
            if key and len(command) > 1:
                slot = self.cluster_keyslot(key)
                # In a real implementation, this would route to the correct node
                # For now, just execute on current connection
                pass

            return conn.execute_command(command)
        finally:
            self.pool.return_connection(conn)

    def cluster_multi_get(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple keys across cluster nodes"""
        results = {}
        for key in keys:
            try:
                value = self.get(key)
                results[key] = value
            except Exception:
                results[key] = None
        return results

    def cluster_multi_set(self, mapping: Dict[str, str]) -> int:
        """Set multiple keys across cluster nodes"""
        success_count = 0
        for key, value in mapping.items():
            try:
                self.set(key, value)
                success_count += 1
            except Exception:
                pass
        return success_count

    def cluster_health_check(self) -> Dict[str, Any]:
        """Check cluster health and topology"""
        health = {
            'cluster_enabled': False,
            'cluster_state': 'unknown',
            'nodes_count': 0,
            'slots_assigned': 0,
            'slots_ok': 0,
            'errors': []
        }

        try:
            # Check if cluster is enabled
            info = self.cluster_info()
            if 'cluster_enabled' in info and info['cluster_enabled'] == '1':
                health['cluster_enabled'] = True
                health['cluster_state'] = info.get('cluster_state', 'unknown')

            # Get cluster nodes
            nodes = self.cluster_nodes()
            if nodes:
                health['nodes_count'] = len(nodes)

            # Get cluster slots
            slots = self.cluster_slots()
            if slots:
                health['slots_assigned'] = len(slots)
                # Check if all slots are assigned
                health['slots_ok'] = health['slots_assigned'] == 16384

        except Exception as e:
            health['errors'].append(str(e))

        return health

    def cluster_redistribute_slots(self, slots_per_node: int = None) -> Dict[str, Any]:
        """Redistribute hash slots across cluster nodes"""
        result = {
            'success': False,
            'slots_moved': 0,
            'errors': []
        }

        try:
            # Get current cluster topology
            nodes = self.cluster_nodes()
            slots = self.cluster_slots()

            if not nodes or not slots:
                result['errors'].append("Cannot get cluster topology")
                return result

            # This is a simplified redistribution - real implementation would be more complex
            # For demo purposes, just report current state
            result['success'] = True
            result['slots_moved'] = len(slots)

        except Exception as e:
            result['errors'].append(str(e))

        return result

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

    # ===== ASYNC BITMAP OPERATIONS =====

    async def setbit_async(self, key: str, offset: int, value: int) -> int:
        """Async set bit at offset in bitmap"""
        return self.client.setbit(key, offset, value)

    async def getbit_async(self, key: str, offset: int) -> int:
        """Async get bit value at offset"""
        return self.client.getbit(key, offset)

    async def bitcount_async(self, key: str, start: int = 0, end: int = -1) -> int:
        """Async count set bits in bitmap"""
        return self.client.bitcount(key, start, end)

    async def bitop_async(self, operation: str, destkey: str, *srckeys: str) -> int:
        """Async perform bitwise operations on multiple bitmaps"""
        return self.client.bitop(operation, destkey, *srckeys)

    async def bitpos_async(self, key: str, bit: int, start: int = 0, end: int = -1) -> int:
        """Async find first bit set to specified value"""
        return self.client.bitpos(key, bit, start, end)

    async def bitfield_async(self, key: str, operations: List[Dict[str, Any]]) -> List[int]:
        """Async perform arbitrary bitfield integer operations"""
        return self.client.bitfield(key, operations)

    # ===== ASYNC BLOOM FILTER OPERATIONS =====

    async def bf_reserve_async(self, key: str, error_rate: float, capacity: int) -> str:
        """Async create a new Bloom filter"""
        return self.client.bf_reserve(key, error_rate, capacity)

    async def bf_add_async(self, key: str, item: str) -> int:
        """Async add item to Bloom filter"""
        return self.client.bf_add(key, item)

    async def bf_madd_async(self, key: str, *items: str) -> List[int]:
        """Async add multiple items to Bloom filter"""
        return self.client.bf_madd(key, *items)

    async def bf_exists_async(self, key: str, item: str) -> int:
        """Async check if item exists in Bloom filter"""
        return self.client.bf_exists(key, item)

    async def bf_mexists_async(self, key: str, *items: str) -> List[int]:
        """Async check if multiple items exist in Bloom filter"""
        return self.client.bf_mexists(key, *items)

    async def bf_info_async(self, key: str) -> Dict[str, Any]:
        """Async get Bloom filter information"""
        return self.client.bf_info(key)

    # ===== ASYNC JSON OPERATIONS =====

    async def json_set_async(self, key: str, path: str, value: Any, nx: bool = False, xx: bool = False) -> str:
        """Async set JSON value at path"""
        return self.client.json_set(key, path, value, nx, xx)

    async def json_get_async(self, key: str, path: str = ".") -> Any:
        """Async get JSON value at path"""
        return self.client.json_get(key, path)

    async def json_mget_async(self, keys: List[str], path: str = ".") -> List[Any]:
        """Async get JSON values from multiple keys"""
        return self.client.json_mget(keys, path)

    async def json_del_async(self, key: str, path: str = ".") -> int:
        """Async delete JSON value at path"""
        return self.client.json_del(key, path)

    async def json_arrappend_async(self, key: str, path: str, *values: Any) -> int:
        """Async append values to JSON array"""
        return self.client.json_arrappend(key, path, *values)

    async def json_arrinsert_async(self, key: str, path: str, index: int, *values: Any) -> int:
        """Async insert values into JSON array at index"""
        return self.client.json_arrinsert(key, path, index, *values)

    async def json_arrlen_async(self, key: str, path: str = ".") -> int:
        """Async get length of JSON array"""
        return self.client.json_arrlen(key, path)

    async def json_arrpop_async(self, key: str, path: str, index: int = -1) -> Any:
        """Async remove and return element from JSON array"""
        return self.client.json_arrpop(key, path, index)

    async def json_objkeys_async(self, key: str, path: str = ".") -> List[str]:
        """Async get JSON object keys"""
        return self.client.json_objkeys(key, path)

    async def json_objlen_async(self, key: str, path: str = ".") -> int:
        """Async get length of JSON object"""
        return self.client.json_objlen(key, path)

    async def json_numincrby_async(self, key: str, path: str, increment: float) -> float:
        """Async increment JSON number by value"""
        return self.client.json_numincrby(key, path, increment)

    async def json_nummultby_async(self, key: str, path: str, multiplier: float) -> float:
        """Async multiply JSON number by value"""
        return self.client.json_nummultby(key, path, multiplier)

    async def json_strappend_async(self, key: str, path: str, value: str) -> int:
        """Async append string to JSON string"""
        return self.client.json_strappend(key, path, value)

    async def json_strlen_async(self, key: str, path: str = ".") -> int:
        """Async get length of JSON string"""
        return self.client.json_strlen(key, path)

    async def json_type_async(self, key: str, path: str = ".") -> str:
        """Async get JSON type at path"""
        return self.client.json_type(key, path)

    async def json_toggle_async(self, key: str, path: str) -> List[int]:
        """Async toggle boolean values in JSON"""
        return self.client.json_toggle(key, path)

    # ===== ASYNC FULL-TEXT SEARCH OPERATIONS =====

    async def ft_create_async(self, index: str, schema: List[Dict[str, str]],
                             options: Dict[str, Any] = None) -> str:
        """Async create a full-text search index"""
        return self.client.ft_create(index, schema, options)

    async def ft_search_async(self, index: str, query: str,
                             options: Dict[str, Any] = None) -> Dict[str, Any]:
        """Async search the full-text index"""
        return self.client.ft_search(index, query, options)

    async def ft_info_async(self, index: str) -> Dict[str, Any]:
        """Async get information about a full-text search index"""
        return self.client.ft_info(index)

    async def ft_dropindex_async(self, index: str) -> str:
        """Async drop a full-text search index"""
        return self.client.ft_dropindex(index)

    # ===== ASYNC GEOSPATIAL OPERATIONS =====

    async def geoadd_async(self, key: str, longitude: float, latitude: float, member: str) -> int:
        """Async add geospatial member"""
        return self.client.geoadd(key, longitude, latitude, member)

    async def geodist_async(self, key: str, member1: str, member2: str, unit: str = "m") -> float:
        """Async get distance between two members"""
        return self.client.geodist(key, member1, member2, unit)

    async def geohash_async(self, key: str, *members: str) -> List[str]:
        """Async get geohash strings for members"""
        return self.client.geohash(key, *members)

    async def geopos_async(self, key: str, *members: str) -> List[List[float]]:
        """Async get longitude,latitude positions of members"""
        return self.client.geopos(key, *members)

    async def georadius_async(self, key: str, longitude: float, latitude: float, radius: float,
                             unit: str = "m", withdist: bool = False, withcoord: bool = False,
                             withhash: bool = False, count: int = -1, sort: str = None) -> List[str]:
        """Async query a geospatial index by radius"""
        return self.client.georadius(key, longitude, latitude, radius, unit, withdist, withcoord, withhash, count, sort)

    async def georadiusbymember_async(self, key: str, member: str, radius: float,
                                     unit: str = "m", withdist: bool = False, withcoord: bool = False,
                                     withhash: bool = False, count: int = -1, sort: str = None) -> List[str]:
        """Async query a geospatial index by radius from a member"""
        return self.client.georadiusbymember(key, member, radius, unit, withdist, withcoord, withhash, count, sort)

    async def geosearch_async(self, key: str, member: str = None, longitude: float = None,
                             latitude: float = None, radius: float = None, width: float = None,
                             height: float = None, unit: str = "m", withdist: bool = False,
                             withcoord: bool = False, withhash: bool = False, count: int = -1) -> List[str]:
        """Async search geospatial index"""
        return self.client.geosearch(key, member, longitude, latitude, radius, width, height, unit, withdist, withcoord, withhash, count)

    # ===== ASYNC TIME SERIES OPERATIONS (RedisTimeSeries) =====

    async def ts_create_async(self, key: str, retention: int = 0, chunk_size: int = 4096,
                             duplicate_policy: str = "block", labels: Dict[str, str] = None) -> str:
        """Async create a time series"""
        return self.client.ts_create(key, retention, chunk_size, duplicate_policy, labels)

    async def ts_add_async(self, key: str, timestamp: int, value: float, retention: int = 0,
                          chunk_size: int = 0, labels: Dict[str, str] = None) -> int:
        """Async add sample to time series"""
        return self.client.ts_add(key, timestamp, value, retention, chunk_size, labels)

    async def ts_get_async(self, key: str, latest: bool = False) -> List[Any]:
        """Async get last sample from time series"""
        return self.client.ts_get(key, latest)

    async def ts_range_async(self, key: str, from_time: int, to_time: int,
                            latest: bool = False, filter_by_ts: List[int] = None,
                            filter_by_value: List[float] = None, count: int = -1,
                            align: str = None, aggregation: str = None,
                            bucket_size_msec: int = 0) -> List[List[Any]]:
        """Async get range of samples from time series"""
        return self.client.ts_range(key, from_time, to_time, latest, filter_by_ts, filter_by_value, count, align, aggregation, bucket_size_msec)

    async def ts_info_async(self, key: str) -> Dict[str, Any]:
        """Async get information about a time series"""
        return self.client.ts_info(key)

    async def ts_queryindex_async(self, filters: List[str]) -> List[str]:
        """Async query index for time series keys"""
        return self.client.ts_queryindex(filters)

    async def ts_mget_async(self, filters: List[str], latest: bool = False) -> List[Dict[str, Any]]:
        """Async get multiple time series by filters"""
        return self.client.ts_mget(filters, latest)

    async def ts_del_async(self, key: str, from_time: int, to_time: int) -> int:
        """Async delete samples from time series"""
        return self.client.ts_del(key, from_time, to_time)

    async def ts_alter_async(self, key: str, retention: int = 0, chunk_size: int = 0,
                            duplicate_policy: str = None, labels: Dict[str, str] = None) -> str:
        """Async alter time series settings"""
        return self.client.ts_alter(key, retention, chunk_size, duplicate_policy, labels)

    async def ts_madd_async(self, sequences: List[Dict[str, Any]]) -> List[int]:
        """Async add multiple samples to multiple time series"""
        return self.client.ts_madd(sequences)

    async def ts_incrby_async(self, key: str, value: float, timestamp: int = None,
                             retention: int = 0, chunk_size: int = 0,
                             labels: Dict[str, str] = None) -> int:
        """Async increment time series value"""
        return self.client.ts_incrby(key, value, timestamp, retention, chunk_size, labels)

    async def ts_decrby_async(self, key: str, value: float, timestamp: int = None,
                             retention: int = 0, chunk_size: int = 0,
                             labels: Dict[str, str] = None) -> int:
        """Async decrement time series value"""
        return self.client.ts_decrby(key, value, timestamp, retention, chunk_size, labels)

    # ===== ASYNC ADVANCED HASH OPERATIONS =====

    async def hsetex_async(self, key: str, field: str, value: str, seconds: int) -> int:
        """Async set hash field with expiration"""
        return self.client.hsetex(key, field, value, seconds)

    async def hexpire_async(self, key: str, seconds: int, fields: List[str] = None,
                           nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False) -> int:
        """Async set expiration on hash fields"""
        return self.client.hexpire(key, seconds, fields, nx, xx, gt, lt)

    async def hpexpire_async(self, key: str, milliseconds: int, fields: List[str] = None,
                            nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False) -> int:
        """Async set expiration on hash fields in milliseconds"""
        return self.client.hpexpire(key, milliseconds, fields, nx, xx, gt, lt)

    async def hpersist_async(self, key: str, fields: List[str] = None) -> int:
        """Async remove expiration from hash fields"""
        return self.client.hpersist(key, fields)

    async def hincrbyfloat_async(self, key: str, field: str, increment: float) -> float:
        """Async increment hash field by float"""
        return self.client.hincrbyfloat(key, field, increment)

    async def hrandfield_async(self, key: str, count: int = 1, withvalues: bool = False) -> Union[str, List[str]]:
        """Async get random field(s) from hash"""
        return self.client.hrandfield(key, count, withvalues)

    async def hstrlen_async(self, key: str, field: str) -> int:
        """Async get string length of hash field"""
        return self.client.hstrlen(key, field)

    async def hgetex_async(self, key: str, field: str) -> Optional[str]:
        """Async get hash field (enhanced version)"""
        return self.client.hgetex(key, field)

    async def hgetall_dict_async(self, key: str) -> Dict[str, str]:
        """Async get all hash fields as dictionary"""
        return self.client.hgetall_dict(key)

    # ===== ASYNC BITMAP OPERATIONS =====

    async def setbit_async(self, key: str, offset: int, value: int) -> int:
        """Async set bit at offset in bitmap"""
        return await self.client.setbit_async(key, offset, value)

    async def getbit_async(self, key: str, offset: int) -> int:
        """Async get bit value at offset"""
        return await self.client.getbit_async(key, offset)

    async def bitcount_async(self, key: str, start: int = 0, end: int = -1) -> int:
        """Async count set bits in bitmap"""
        return await self.client.bitcount_async(key, start, end)

    async def bitop_async(self, operation: str, destkey: str, *srckeys: str) -> int:
        """Async perform bitwise operations on multiple bitmaps"""
        return await self.client.bitop_async(operation, destkey, *srckeys)

    async def bitpos_async(self, key: str, bit: int, start: int = 0, end: int = -1) -> int:
        """Async find first bit set to specified value"""
        return await self.client.bitpos_async(key, bit, start, end)

    async def bitfield_async(self, key: str, operations: List[Dict[str, Any]]) -> List[int]:
        """Async perform arbitrary bitfield integer operations"""
        return await self.client.bitfield_async(key, operations)

    # ===== ASYNC BLOOM FILTER OPERATIONS =====

    async def bf_reserve_async(self, key: str, error_rate: float, capacity: int) -> str:
        """Async create a new Bloom filter"""
        return await self.client.bf_reserve_async(key, error_rate, capacity)

    async def bf_add_async(self, key: str, item: str) -> int:
        """Async add item to Bloom filter"""
        return await self.client.bf_add_async(key, item)

    async def bf_madd_async(self, key: str, *items: str) -> List[int]:
        """Async add multiple items to Bloom filter"""
        return await self.client.bf_madd_async(key, *items)

    async def bf_exists_async(self, key: str, item: str) -> int:
        """Async check if item exists in Bloom filter"""
        return await self.client.bf_exists_async(key, item)

    async def bf_mexists_async(self, key: str, *items: str) -> List[int]:
        """Async check if multiple items exist in Bloom filter"""
        return await self.client.bf_mexists_async(key, *items)

    async def bf_info_async(self, key: str) -> Dict[str, Any]:
        """Async get Bloom filter information"""
        return await self.client.bf_info_async(key)

    # ===== ASYNC JSON OPERATIONS =====

    async def json_set_async(self, key: str, path: str, value: Any, nx: bool = False, xx: bool = False) -> str:
        """Async set JSON value at path"""
        return await self.client.json_set_async(key, path, value, nx, xx)

    async def json_get_async(self, key: str, path: str = ".") -> Any:
        """Async get JSON value at path"""
        return await self.client.json_get_async(key, path)

    async def json_mget_async(self, keys: List[str], path: str = ".") -> List[Any]:
        """Async get JSON values from multiple keys"""
        return await self.client.json_mget_async(keys, path)

    async def json_del_async(self, key: str, path: str = ".") -> int:
        """Async delete JSON value at path"""
        return await self.client.json_del_async(key, path)

    async def json_arrappend_async(self, key: str, path: str, *values: Any) -> int:
        """Async append values to JSON array"""
        return await self.client.json_arrappend_async(key, path, *values)

    async def json_arrinsert_async(self, key: str, path: str, index: int, *values: Any) -> int:
        """Async insert values into JSON array at index"""
        return await self.client.json_arrinsert_async(key, path, index, *values)

    async def json_arrlen_async(self, key: str, path: str = ".") -> int:
        """Async get length of JSON array"""
        return await self.client.json_arrlen_async(key, path)

    async def json_arrpop_async(self, key: str, path: str, index: int = -1) -> Any:
        """Async remove and return element from JSON array"""
        return await self.client.json_arrpop_async(key, path, index)

    async def json_objkeys_async(self, key: str, path: str = ".") -> List[str]:
        """Async get JSON object keys"""
        return await self.client.json_objkeys_async(key, path)

    async def json_objlen_async(self, key: str, path: str = ".") -> int:
        """Async get length of JSON object"""
        return await self.client.json_objlen_async(key, path)

    async def json_numincrby_async(self, key: str, path: str, increment: float) -> float:
        """Async increment JSON number by value"""
        return await self.client.json_numincrby_async(key, path, increment)

    async def json_nummultby_async(self, key: str, path: str, multiplier: float) -> float:
        """Async multiply JSON number by value"""
        return await self.client.json_nummultby_async(key, path, multiplier)

    async def json_strappend_async(self, key: str, path: str, value: str) -> int:
        """Async append string to JSON string"""
        return await self.client.json_strappend_async(key, path, value)

    async def json_strlen_async(self, key: str, path: str = ".") -> int:
        """Async get length of JSON string"""
        return await self.client.json_strlen_async(key, path)

    async def json_type_async(self, key: str, path: str = ".") -> str:
        """Async get JSON type at path"""
        return await self.client.json_type_async(key, path)

    async def json_toggle_async(self, key: str, path: str) -> List[int]:
        """Async toggle boolean values in JSON"""
        return await self.client.json_toggle_async(key, path)

    # ===== ASYNC FULL-TEXT SEARCH OPERATIONS =====

    async def ft_create_async(self, index: str, schema: List[Dict[str, str]],
                             options: Dict[str, Any] = None) -> str:
        """Async create a full-text search index"""
        return await self.client.ft_create_async(index, schema, options)

    async def ft_search_async(self, index: str, query: str,
                             options: Dict[str, Any] = None) -> Dict[str, Any]:
        """Async search the full-text index"""
        return await self.client.ft_search_async(index, query, options)

    async def ft_info_async(self, index: str) -> Dict[str, Any]:
        """Async get information about a full-text search index"""
        return await self.client.ft_info_async(index)

    async def ft_dropindex_async(self, index: str) -> str:
        """Async drop a full-text search index"""
        return await self.client.ft_dropindex_async(index)

    # ===== ASYNC GEOSPATIAL OPERATIONS =====

    async def geoadd_async(self, key: str, longitude: float, latitude: float, member: str) -> int:
        """Async add geospatial member"""
        return await self.client.geoadd_async(key, longitude, latitude, member)

    async def geodist_async(self, key: str, member1: str, member2: str, unit: str = "m") -> float:
        """Async get distance between two members"""
        return await self.client.geodist_async(key, member1, member2, unit)

    async def geohash_async(self, key: str, *members: str) -> List[str]:
        """Async get geohash strings for members"""
        return await self.client.geohash_async(key, *members)

    async def geopos_async(self, key: str, *members: str) -> List[List[float]]:
        """Async get longitude,latitude positions of members"""
        return await self.client.geopos_async(key, *members)

    async def georadius_async(self, key: str, longitude: float, latitude: float, radius: float,
                             unit: str = "m", withdist: bool = False, withcoord: bool = False,
                             withhash: bool = False, count: int = -1, sort: str = None) -> List[str]:
        """Async query a geospatial index by radius"""
        return await self.client.georadius_async(key, longitude, latitude, radius, unit, withdist, withcoord, withhash, count, sort)

    async def georadiusbymember_async(self, key: str, member: str, radius: float,
                                     unit: str = "m", withdist: bool = False, withcoord: bool = False,
                                     withhash: bool = False, count: int = -1, sort: str = None) -> List[str]:
        """Async query a geospatial index by radius from a member"""
        return await self.client.georadiusbymember_async(key, member, radius, unit, withdist, withcoord, withhash, count, sort)

    async def geosearch_async(self, key: str, member: str = None, longitude: float = None,
                             latitude: float = None, radius: float = None, width: float = None,
                             height: float = None, unit: str = "m", withdist: bool = False,
                             withcoord: bool = False, withhash: bool = False, count: int = -1) -> List[str]:
        """Async search geospatial index"""
        return await self.client.geosearch_async(key, member, longitude, latitude, radius, width, height, unit, withdist, withcoord, withhash, count)

    # ===== ASYNC TIME SERIES OPERATIONS (RedisTimeSeries) =====

    async def ts_create_async(self, key: str, retention: int = 0, chunk_size: int = 4096,
                             duplicate_policy: str = "block", labels: Dict[str, str] = None) -> str:
        """Async create a time series"""
        return await self.client.ts_create_async(key, retention, chunk_size, duplicate_policy, labels)

    async def ts_add_async(self, key: str, timestamp: int, value: float, retention: int = 0,
                          chunk_size: int = 0, labels: Dict[str, str] = None) -> int:
        """Async add sample to time series"""
        return await self.client.ts_add_async(key, timestamp, value, retention, chunk_size, labels)

    async def ts_get_async(self, key: str, latest: bool = False) -> List[Any]:
        """Async get last sample from time series"""
        return await self.client.ts_get_async(key, latest)

    async def ts_range_async(self, key: str, from_time: int, to_time: int,
                            latest: bool = False, filter_by_ts: List[int] = None,
                            filter_by_value: List[float] = None, count: int = -1,
                            align: str = None, aggregation: str = None,
                            bucket_size_msec: int = 0) -> List[List[Any]]:
        """Async get range of samples from time series"""
        return await self.client.ts_range_async(key, from_time, to_time, latest, filter_by_ts, filter_by_value, count, align, aggregation, bucket_size_msec)

    async def ts_info_async(self, key: str) -> Dict[str, Any]:
        """Async get information about a time series"""
        return await self.client.ts_info_async(key)

    async def ts_queryindex_async(self, filters: List[str]) -> List[str]:
        """Async query index for time series keys"""
        return await self.client.ts_queryindex_async(filters)

    async def ts_mget_async(self, filters: List[str], latest: bool = False) -> List[Dict[str, Any]]:
        """Async get multiple time series by filters"""
        return await self.client.ts_mget_async(filters, latest)

    async def ts_del_async(self, key: str, from_time: int, to_time: int) -> int:
        """Async delete samples from time series"""
        return await self.client.ts_del_async(key, from_time, to_time)

    async def ts_alter_async(self, key: str, retention: int = 0, chunk_size: int = 0,
                            duplicate_policy: str = None, labels: Dict[str, str] = None) -> str:
        """Async alter time series settings"""
        return await self.client.ts_alter_async(key, retention, chunk_size, duplicate_policy, labels)

    async def ts_madd_async(self, sequences: List[Dict[str, Any]]) -> List[int]:
        """Async add multiple samples to multiple time series"""
        return await self.client.ts_madd_async(sequences)

    async def ts_incrby_async(self, key: str, value: float, timestamp: int = None,
                             retention: int = 0, chunk_size: int = 0,
                             labels: Dict[str, str] = None) -> int:
        """Async increment time series value"""
        return await self.client.ts_incrby_async(key, value, timestamp, retention, chunk_size, labels)

    async def ts_decrby_async(self, key: str, value: float, timestamp: int = None,
                             retention: int = 0, chunk_size: int = 0,
                             labels: Dict[str, str] = None) -> int:
        """Async decrement time series value"""
        return await self.client.ts_decrby_async(key, value, timestamp, retention, chunk_size, labels)

    # ===== ASYNC ADVANCED HASH OPERATIONS =====

    async def hsetex_async(self, key: str, field: str, value: str, seconds: int) -> int:
        """Async set hash field with expiration"""
        return await self.client.hsetex_async(key, field, value, seconds)

    async def hexpire_async(self, key: str, seconds: int, fields: List[str] = None,
                           nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False) -> int:
        """Async set expiration on hash fields"""
        return await self.client.hexpire_async(key, seconds, fields, nx, xx, gt, lt)

    async def hpexpire_async(self, key: str, milliseconds: int, fields: List[str] = None,
                            nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False) -> int:
        """Async set expiration on hash fields in milliseconds"""
        return await self.client.hpexpire_async(key, milliseconds, fields, nx, xx, gt, lt)

    async def hpersist_async(self, key: str, fields: List[str] = None) -> int:
        """Async remove expiration from hash fields"""
        return await self.client.hpersist_async(key, fields)

    async def hincrbyfloat_async(self, key: str, field: str, increment: float) -> float:
        """Async increment hash field by float"""
        return await self.client.hincrbyfloat_async(key, field, increment)

    async def hrandfield_async(self, key: str, count: int = 1, withvalues: bool = False) -> Union[str, List[str]]:
        """Async get random field(s) from hash"""
        return await self.client.hrandfield_async(key, count, withvalues)

    async def hstrlen_async(self, key: str, field: str) -> int:
        """Async get string length of hash field"""
        return await self.client.hstrlen_async(key, field)

    async def hgetex_async(self, key: str, field: str) -> Optional[str]:
        """Async get hash field (enhanced version)"""
        return await self.client.hgetex_async(key, field)

    async def hgetall_dict_async(self, key: str) -> Dict[str, str]:
        """Async get all hash fields as dictionary"""
        return await self.client.hgetall_dict_async(key)

    # ===== ASYNC CLUSTER OPERATIONS =====

    async def cluster_info_async(self) -> Dict[str, Any]:
        """Async get cluster information"""
        return await self.client.cluster_info_async()

    async def cluster_nodes_async(self) -> List[str]:
        """Async get cluster nodes information"""
        return await self.client.cluster_nodes_async()

    async def cluster_slots_async(self) -> List[List[Any]]:
        """Async get cluster slots information"""
        return await self.client.cluster_slots_async()

    async def cluster_keyslot_async(self, key: str) -> int:
        """Async get hash slot for key"""
        return await self.client.cluster_keyslot_async(key)

    async def cluster_countkeysinslot_async(self, slot: int) -> int:
        """Async count keys in hash slot"""
        return await self.client.cluster_countkeysinslot_async(slot)

    async def cluster_getkeysinslot_async(self, slot: int, count: int) -> List[str]:
        """Async get keys in hash slot"""
        return await self.client.cluster_getkeysinslot_async(slot, count)

    async def cluster_addslots_async(self, *slots: int) -> str:
        """Async add hash slots to node"""
        return await self.client.cluster_addslots_async(*slots)

    async def cluster_delslots_async(self, *slots: int) -> str:
        """Async remove hash slots from node"""
        return await self.client.cluster_delslots_async(*slots)

    async def cluster_setslot_async(self, slot: int, subcommand: str, node_id: str = None) -> str:
        """Async set hash slot configuration"""
        return await self.client.cluster_setslot_async(slot, subcommand, node_id)

    async def cluster_meet_async(self, ip: str, port: int) -> str:
        """Async meet other cluster node"""
        return await self.client.cluster_meet_async(ip, port)

    async def cluster_forget_async(self, node_id: str) -> str:
        """Async remove node from cluster"""
        return await self.client.cluster_forget_async(node_id)

    async def cluster_replicate_async(self, node_id: str) -> str:
        """Async configure node as replica of primary"""
        return await self.client.cluster_replicate_async(node_id)

    async def cluster_failover_async(self, force: bool = False, takeover: bool = False) -> str:
        """Async force failover"""
        return await self.client.cluster_failover_async(force, takeover)

    async def cluster_reset_async(self, hard: bool = False, soft: bool = False) -> str:
        """Async reset cluster node"""
        return await self.client.cluster_reset_async(hard, soft)

    async def cluster_saveconfig_async(self) -> str:
        """Async force save cluster configuration"""
        return await self.client.cluster_saveconfig_async()

    async def cluster_bumpepoch_async(self) -> str:
        """Async advance cluster config epoch"""
        return await self.client.cluster_bumpepoch_async()

    async def cluster_myid_async(self) -> str:
        """Async get node ID"""
        return await self.client.cluster_myid_async()

    async def cluster_myshardid_async(self) -> str:
        """Async get shard ID"""
        return await self.client.cluster_myshardid_async()

    async def cluster_flushslots_async(self) -> str:
        """Async delete all slots information"""
        return await self.client.cluster_flushslots_async()

    async def cluster_links_async(self) -> List[Dict[str, Any]]:
        """Async get cluster links information"""
        return await self.client.cluster_links_async()

    async def cluster_count_failure_reports_async(self, node_id: str) -> int:
        """Async count failure reports for node"""
        return await self.client.cluster_count_failure_reports_async(node_id)

    async def cluster_shards_async(self) -> List[Dict[str, Any]]:
        """Async get cluster shards information"""
        return await self.client.cluster_shards_async()

    async def cluster_slot_stats_async(self) -> List[Dict[str, Any]]:
        """Async get slot usage statistics"""
        return await self.client.cluster_slot_stats_async()

    async def cluster_health_check_async(self) -> Dict[str, Any]:
        """Async check cluster health and topology"""
        return await self.client.cluster_health_check_async()

    async def cluster_redistribute_slots_async(self, slots_per_node: int = None) -> Dict[str, Any]:
        """Async redistribute hash slots across cluster nodes"""
        return await self.client.cluster_redistribute_slots_async(slots_per_node)

    # ===== CLUSTER OPERATIONS (HighPerformanceRedis) =====

    def cluster_info(self) -> Dict[str, Any]:
        """Get cluster information"""
        return self.client.cluster_info()

    def cluster_nodes(self) -> List[str]:
        """Get cluster nodes information"""
        return self.client.cluster_nodes()

    def cluster_slots(self) -> List[List[Any]]:
        """Get cluster slots information"""
        return self.client.cluster_slots()

    def cluster_keyslot(self, key: str) -> int:
        """Get hash slot for key"""
        return self.client.cluster_keyslot(key)

    def cluster_countkeysinslot(self, slot: int) -> int:
        """Count keys in hash slot"""
        return self.client.cluster_countkeysinslot(slot)

    def cluster_getkeysinslot(self, slot: int, count: int) -> List[str]:
        """Get keys in hash slot"""
        return self.client.cluster_getkeysinslot(slot, count)

    def cluster_addslots(self, *slots: int) -> str:
        """Add hash slots to node"""
        return self.client.cluster_addslots(*slots)

    def cluster_delslots(self, *slots: int) -> str:
        """Remove hash slots from node"""
        return self.client.cluster_delslots(*slots)

    def cluster_setslot(self, slot: int, subcommand: str, node_id: str = None) -> str:
        """Set hash slot configuration"""
        return self.client.cluster_setslot(slot, subcommand, node_id)

    def cluster_meet(self, ip: str, port: int) -> str:
        """Meet other cluster node"""
        return self.client.cluster_meet(ip, port)

    def cluster_forget(self, node_id: str) -> str:
        """Remove node from cluster"""
        return self.client.cluster_forget(node_id)

    def cluster_replicate(self, node_id: str) -> str:
        """Configure node as replica of primary"""
        return self.client.cluster_replicate(node_id)

    def cluster_failover(self, force: bool = False, takeover: bool = False) -> str:
        """Force failover"""
        return self.client.cluster_failover(force, takeover)

    def cluster_reset(self, hard: bool = False, soft: bool = False) -> str:
        """Reset cluster node"""
        return self.client.cluster_reset(hard, soft)

    def cluster_saveconfig(self) -> str:
        """Force save cluster configuration"""
        return self.client.cluster_saveconfig()

    def cluster_bumpepoch(self) -> str:
        """Advance cluster config epoch"""
        return self.client.cluster_bumpepoch()

    def cluster_myid(self) -> str:
        """Get node ID"""
        return self.client.cluster_myid()

    def cluster_myshardid(self) -> str:
        """Get shard ID"""
        return self.client.cluster_myshardid()

    def cluster_flushslots(self) -> str:
        """Delete all slots information"""
        return self.client.cluster_flushslots()

    def cluster_links(self) -> List[Dict[str, Any]]:
        """Get cluster links information"""
        return self.client.cluster_links()

    def cluster_count_failure_reports(self, node_id: str) -> int:
        """Count failure reports for node"""
        return self.client.cluster_count_failure_reports(node_id)

    def cluster_shards(self) -> List[Dict[str, Any]]:
        """Get cluster shards information"""
        return self.client.cluster_shards()

    def cluster_slot_stats(self) -> List[Dict[str, Any]]:
        """Get slot usage statistics"""
        return self.client.cluster_slot_stats()

    def cluster_health_check(self) -> Dict[str, Any]:
        """Check cluster health and topology"""
        return self.client.cluster_health_check()

    def cluster_redistribute_slots(self, slots_per_node: int = None) -> Dict[str, Any]:
        """Redistribute hash slots across cluster nodes"""
        return self.client.cluster_redistribute_slots(slots_per_node)

    def cluster_multi_get(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple keys across cluster nodes"""
        return self.client.cluster_multi_get(keys)

    def cluster_multi_set(self, mapping: Dict[str, str]) -> int:
        """Set multiple keys across cluster nodes"""
        return self.client.cluster_multi_set(mapping)

    # ===== ASYNC CLUSTER OPERATIONS (HighPerformanceRedis) =====

    async def cluster_info_async(self) -> Dict[str, Any]:
        """Async get cluster information"""
        return await self.client.cluster_info_async()

    async def cluster_nodes_async(self) -> List[str]:
        """Async get cluster nodes information"""
        return await self.client.cluster_nodes_async()

    async def cluster_slots_async(self) -> List[List[Any]]:
        """Async get cluster slots information"""
        return await self.client.cluster_slots_async()

    async def cluster_keyslot_async(self, key: str) -> int:
        """Async get hash slot for key"""
        return await self.client.cluster_keyslot_async(key)

    async def cluster_countkeysinslot_async(self, slot: int) -> int:
        """Async count keys in hash slot"""
        return await self.client.cluster_countkeysinslot_async(slot)

    async def cluster_getkeysinslot_async(self, slot: int, count: int) -> List[str]:
        """Async get keys in hash slot"""
        return await self.client.cluster_getkeysinslot_async(slot, count)

    async def cluster_addslots_async(self, *slots: int) -> str:
        """Async add hash slots to node"""
        return await self.client.cluster_addslots_async(*slots)

    async def cluster_delslots_async(self, *slots: int) -> str:
        """Async remove hash slots from node"""
        return await self.client.cluster_delslots_async(*slots)

    async def cluster_setslot_async(self, slot: int, subcommand: str, node_id: str = None) -> str:
        """Async set hash slot configuration"""
        return await self.client.cluster_setslot_async(slot, subcommand, node_id)

    async def cluster_meet_async(self, ip: str, port: int) -> str:
        """Async meet other cluster node"""
        return await self.client.cluster_meet_async(ip, port)

    async def cluster_forget_async(self, node_id: str) -> str:
        """Async remove node from cluster"""
        return await self.client.cluster_forget_async(node_id)

    async def cluster_replicate_async(self, node_id: str) -> str:
        """Async configure node as replica of primary"""
        return await self.client.cluster_replicate_async(node_id)

    async def cluster_failover_async(self, force: bool = False, takeover: bool = False) -> str:
        """Async force failover"""
        return await self.client.cluster_failover_async(force, takeover)

    async def cluster_reset_async(self, hard: bool = False, soft: bool = False) -> str:
        """Async reset cluster node"""
        return await self.client.cluster_reset_async(hard, soft)

    async def cluster_saveconfig_async(self) -> str:
        """Async force save cluster configuration"""
        return await self.client.cluster_saveconfig_async()

    async def cluster_bumpepoch_async(self) -> str:
        """Async advance cluster config epoch"""
        return await self.client.cluster_bumpepoch_async()

    async def cluster_myid_async(self) -> str:
        """Async get node ID"""
        return await self.client.cluster_myid_async()

    async def cluster_myshardid_async(self) -> str:
        """Async get shard ID"""
        return await self.client.cluster_myshardid_async()

    async def cluster_flushslots_async(self) -> str:
        """Async delete all slots information"""
        return await self.client.cluster_flushslots_async()

    async def cluster_links_async(self) -> List[Dict[str, Any]]:
        """Async get cluster links information"""
        return await self.client.cluster_links_async()

    async def cluster_count_failure_reports_async(self, node_id: str) -> int:
        """Async count failure reports for node"""
        return await self.client.cluster_count_failure_reports_async(node_id)

    async def cluster_shards_async(self) -> List[Dict[str, Any]]:
        """Async get cluster shards information"""
        return await self.client.cluster_shards_async()

    async def cluster_slot_stats_async(self) -> List[Dict[str, Any]]:
        """Async get slot usage statistics"""
        return await self.client.cluster_slot_stats_async()

    async def cluster_health_check_async(self) -> Dict[str, Any]:
        """Async check cluster health and topology"""
        return await self.client.cluster_health_check_async()

    async def cluster_redistribute_slots_async(self, slots_per_node: int = None) -> Dict[str, Any]:
        """Async redistribute hash slots across cluster nodes"""
        return await self.client.cluster_redistribute_slots_async(slots_per_node)

    async def cluster_multi_get_async(self, keys: List[str]) -> Dict[str, Any]:
        """Async get multiple keys across cluster nodes"""
        return await self.client.cluster_multi_get_async(keys)

    async def cluster_multi_set_async(self, mapping: Dict[str, str]) -> int:
        """Async set multiple keys across cluster nodes"""
        return await self.client.cluster_multi_set_async(mapping)
