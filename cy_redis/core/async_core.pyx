"""
Async Redis Core - Async Redis client using thread pools with uvloop.
Provides asynchronous Redis operations without blocking the event loop.
"""

import asyncio

try:
    import uvloop
    _HAS_UVLOOP = True
except ImportError:
    _HAS_UVLOOP = False
from concurrent.futures import ThreadPoolExecutor

from .cy_redis_client import CyRedisConnection, CyRedisConnectionPool


class AsyncMessage:
    def __init__(self, data=None):
        self.data = data


class AsyncMessageQueue:
    """Simple async-safe queue using a list and asyncio locks."""

    def __init__(self, capacity: int = 1024):
        # Precondition: a bounded queue needs a positive capacity.
        assert capacity > 0, "queue capacity must be positive"
        self.capacity = capacity
        self._queue = []
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)
        self._not_full = asyncio.Condition(self._lock)

    async def put(self, msg: AsyncMessage):
        # Precondition: only AsyncMessage values flow through this queue.
        assert msg is not None, "cannot enqueue None"
        async with self._lock:
            # Bounded wait loop: blocks only while full, woken by get()'s
            # not_full.notify(); capacity is the explicit upper bound.
            while len(self._queue) >= self.capacity:
                await self._not_full.wait()
            self._queue.append(msg)
            # Invariant: the queue never overruns its declared capacity.
            assert len(self._queue) <= self.capacity, "queue overflow"
            self._not_empty.notify()

    async def get(self) -> AsyncMessage:
        async with self._lock:
            # Bounded wait loop: blocks only while empty, woken by put()'s
            # not_empty.notify().
            while not self._queue:
                await self._not_empty.wait()
            msg = self._queue.pop(0)
            # Postcondition: a dequeued message is always a real value.
            assert msg is not None, "dequeued a None message"
            self._not_full.notify()
            return msg

    @property
    def queue(self):
        return self

    @property
    def size(self) -> int:
        return len(self._queue)


class AsyncRedisConnection:
    """Async Redis connection using thread pool"""

    def __init__(self, host="localhost", port=6379):
        # Precondition: a valid TCP port is 1..65535.
        assert 0 < port <= 65535, "port out of range"
        assert host is not None, "host must be set"
        self.host = host
        self.port = port
        self._connection = None

    def _get_connection(self):
        """Lazy connection creation"""
        if self._connection is None:
            self._connection = CyRedisConnection(self.host, self.port)
        # Postcondition: callers always receive a live connection object.
        assert self._connection is not None, "failed to create connection"
        return self._connection

    def connect(self) -> int:
        """Establish connection; returns 0 on success."""
        conn = self._get_connection()
        result = conn.connect()
        return 0 if result == 0 else -1

    def disconnect(self):
        """Close the underlying connection."""
        if self._connection is not None:
            self._connection.disconnect()
            self._connection = None

    def execute_command(self, command):
        """Execute command synchronously"""
        # Precondition: a command always carries at least its name.
        assert command is not None, "command must not be None"
        assert len(command) > 0, "command requires at least its name"
        return self._get_connection().execute_command(command)

    async def execute_command_async(self, command, executor=None):
        """Execute command asynchronously using the provided (or running loop's) executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, self.execute_command, command)


class AsyncRedisClient:
    """High-level async Redis client using thread pools with uvloop"""

    def __init__(self, host="localhost", port=6379, max_connections=10):
        # Preconditions: valid TCP port and a positive pool capacity.
        assert 0 < port <= 65535, "port out of range"
        assert max_connections > 0, "max_connections must be positive"
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.connections = []
        self.executor = None
        self._loop = None
        self._pool = None
        self.running = False

    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_loop()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def _ensure_loop(self):
        """Initialise executor and optionally install uvloop if not already running."""
        if self.executor is None:
            self.executor = ThreadPoolExecutor(max_workers=self.max_connections)
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            if _HAS_UVLOOP:
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                asyncio.set_event_loop(uvloop.new_event_loop())
            self._loop = asyncio.get_event_loop()

    def start_workers(self):
        """Start background worker threads."""
        if self.executor is None:
            self.executor = ThreadPoolExecutor(max_workers=self.max_connections)
        self.running = True

    def stop_workers(self):
        """Stop background worker threads."""
        self.running = False
        if self.executor is not None:
            self.executor.shutdown(wait=True)
            self.executor = None

    async def close(self):
        """Close the client and cleanup resources."""
        self.running = False
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None
        self._pool = None

    def _get_connection(self):
        """Get a CyRedisConnectionPool-backed connection."""
        if self._pool is None:
            self._pool = CyRedisConnectionPool(
                self.host, self.port, self.max_connections
            )
        return self._pool.get_connection()

    def _return_connection(self, conn):
        if self._pool is not None and conn is not None:
            self._pool.return_connection(conn)

    async def execute(self, command: list):
        """Execute a Redis command (list form) asynchronously."""
        # Precondition: a command always carries at least its name.
        assert command is not None, "command must not be None"
        assert len(command) > 0, "command requires at least its name"
        await self._ensure_loop()
        conn = self._get_connection()
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                self.executor, conn.execute_command, command
            )
        finally:
            self._return_connection(conn)

    # High-level Redis operations
    async def set(self, key: str, value: str) -> bool:
        result = await self.execute(['SET', key, str(value)])
        return result == 'OK'

    async def get(self, key: str):
        return await self.execute(['GET', key])

    async def delete(self, key: str) -> int:
        result = await self.execute(['DEL', key])
        return int(result) if result is not None else 0

    async def incr(self, key: str) -> int:
        result = await self.execute(['INCR', key])
        return int(result) if result is not None else 0

    async def exists(self, key: str) -> bool:
        result = await self.execute(['EXISTS', key])
        return bool(int(result)) if result is not None else False

    async def ping(self) -> str:
        return await self.execute(['PING'])

    async def info(self, section: str = '') -> str:
        cmd = ['INFO', section] if section else ['INFO']
        return await self.execute(cmd)


class AsyncRedisWrapper:
    """Facade matching tests expectations, built on AsyncRedisClient."""

    def __init__(self, host: str = "localhost", port: int = 6379, max_connections: int = 10):
        self._client = AsyncRedisClient(host=host, port=port, max_connections=max_connections)

    async def set(self, key: str, value: str):
        return await self._client.set(key, value)

    async def get(self, key: str):
        return await self._client.get(key)

    async def delete(self, key: str):
        return await self._client.delete(key)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.close()


# Exception classes
class RedisError(Exception):
    """Redis operation error"""
    pass

class ConnectionError(RedisError):
    """Redis connection error"""
    pass