"""
Async Redis Core - Async Redis client using thread pools with uvloop.
Provides asynchronous Redis operations without blocking the event loop.
"""

import asyncio
import uvloop
from concurrent.futures import ThreadPoolExecutor
from .cy_redis_client import CyRedisConnection


class AsyncRedisConnection:
    """Async Redis connection using thread pool"""

    def __init__(self, host="localhost", port=6379):
        self.host = host
        self.port = port
        self._connection = None

    def _get_connection(self):
        """Lazy connection creation"""
        if self._connection is None:
            self._connection = CyRedisConnection(self.host, self.port)
        return self._connection

    def execute_command(self, command):
        """Execute command synchronously"""
        return self._get_connection().execute_command(command)

    async def execute_command_async(self, command):
        """Execute command asynchronously using thread pool"""
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=1)

        try:
            result = await loop.run_in_executor(executor, self.execute_command, command)
            return result
        finally:
            executor.shutdown(wait=True)


class AsyncRedisClient:
    """High-level async Redis client using thread pools with uvloop"""

    def __init__(self, host="localhost", port=6379, max_connections=10):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.connections = []
        self.executor = None
        self._loop = None

    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_loop()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def _ensure_loop(self):
        """Ensure we have a uvloop event loop"""
        try:
            loop = asyncio.get_running_loop()
            if not isinstance(loop, uvloop.Loop):
                # Replace with uvloop if not already using it
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                # Can't replace running loop, so we'll work with what we have
        except RuntimeError:
            # No running loop, create uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            asyncio.set_event_loop(uvloop.new_event_loop())

        self._loop = asyncio.get_event_loop()
        if self.executor is None:
            self.executor = ThreadPoolExecutor(max_workers=self.max_connections)

    async def close(self):
        """Close the client and cleanup resources"""
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None

    def _get_connection(self):
        """Get or create a connection from pool"""
        # Simple implementation - just create new connections up to max
        if len(self.connections) < self.max_connections:
            conn = AsyncRedisConnection(self.host, self.port)
            self.connections.append(conn)
            return conn

        # Round-robin through existing connections
        return self.connections[len(self.connections) % self.max_connections]

    async def execute(self, command):
        """Execute Redis command asynchronously using thread pool"""
        await self._ensure_loop()

        conn = self._get_connection()
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self.executor, conn.execute_command, command)
        return result

    # High-level Redis operations
    async def set(self, key: str, value: str) -> bool:
        """Set a key-value pair"""
        result = await self.execute(f"SET {key} {value}")
        return result == "OK"

    async def get(self, key: str) -> str:
        """Get the value of a key"""
        return await self.execute(f"GET {key}")

    async def delete(self, key: str) -> int:
        """Delete a key"""
        result = await self.execute(f"DEL {key}")
        return int(result) if result else 0

    async def incr(self, key: str) -> int:
        """Increment a key"""
        result = await self.execute(f"INCR {key}")
        return int(result) if result else 0

    async def exists(self, key: str) -> bool:
        """Check if a key exists"""
        result = await self.execute(f"EXISTS {key}")
        return bool(int(result) if result else 0)

    async def ping(self) -> str:
        """Ping Redis server"""
        return await self.execute("PING")

    async def info(self, section: str = "") -> str:
        """Get Redis info"""
        cmd = f"INFO {section}" if section else "INFO"
        return await self.execute(cmd)


# Exception classes
class RedisError(Exception):
    """Redis operation error"""
    pass

class ConnectionError(RedisError):
    """Redis connection error"""
    pass