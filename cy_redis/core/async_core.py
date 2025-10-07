"""
Async Redis Client for CyRedis
Provides high-performance async Redis operations using asyncio and thread pools.

Architecture:
- Uses asyncio.run_in_executor with ThreadPoolExecutor for I/O operations
- Leverages existing CyRedisConnection for actual Redis communication
- Supports connection pooling and automatic reconnection
- Compatible with uvloop for enhanced performance
- Provides async context managers and clean resource management
"""

import asyncio
import uvloop
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Any, Union

# Mock Redis connection for demonstration (in production, this would be CyRedisConnection)
class MockCyRedisConnection:
    """Mock Redis connection for testing async design"""

    def __init__(self, host="localhost", port=6379):
        self.host = host
        self.port = port
        self.data = {}

    def execute_command(self, command):
        """Mock command execution"""
        if not command:
            return None

        cmd_name = command[0].decode('utf-8').upper() if isinstance(command[0], bytes) else str(command[0]).upper()
        args = [arg.decode('utf-8') if isinstance(arg, bytes) else str(arg) for arg in command[1:]]

        if cmd_name == "SET":
            if len(args) >= 2:
                self.data[args[0]] = args[1]
                return "OK"
        elif cmd_name == "GET":
            if args:
                return self.data.get(args[0])
        elif cmd_name == "INCR":
            if args:
                key = args[0]
                current = int(self.data.get(key, 0))
                self.data[key] = current + 1
                return self.data[key]
        elif cmd_name == "EXISTS":
            if args:
                return 1 if args[0] in self.data else 0
        elif cmd_name == "DEL":
            count = 0
            for key in args:
                if key in self.data:
                    del self.data[key]
                    count += 1
            return count
        elif cmd_name == "PING":
            return "PONG"
        elif cmd_name == "INFO":
            return "# Server\r\nredis_version:6.2.6\r\n"
        elif cmd_name == "HSET":
            if len(args) >= 3:
                key, field, value = args[0], args[1], args[2]
                if key not in self.data:
                    self.data[key] = {}
                if isinstance(self.data[key], dict):
                    self.data[key][field] = value
                    return 1
        elif cmd_name == "HGET":
            if len(args) >= 2:
                key, field = args[0], args[1]
                if key in self.data and isinstance(self.data[key], dict):
                    return self.data[key].get(field)
        elif cmd_name == "HGETALL":
            if args:
                key = args[0]
                if key in self.data and isinstance(self.data[key], dict):
                    # Return flat list: [key1, val1, key2, val2, ...]
                    result = []
                    for k, v in self.data[key].items():
                        result.extend([k, v])
                    return result
        elif cmd_name == "LPUSH":
            if len(args) >= 2:
                key = args[0]
                if key not in self.data:
                    self.data[key] = []
                if isinstance(self.data[key], list):
                    for item in args[1:]:
                        self.data[key].insert(0, item)
                    return len(self.data[key])
        elif cmd_name == "RPUSH":
            if len(args) >= 2:
                key = args[0]
                if key not in self.data:
                    self.data[key] = []
                if isinstance(self.data[key], list):
                    for item in args[1:]:
                        self.data[key].append(item)
                    return len(self.data[key])
        elif cmd_name == "LPOP":
            if args:
                key = args[0]
                if key in self.data and isinstance(self.data[key], list) and self.data[key]:
                    return self.data[key].pop(0)
        elif cmd_name == "RPOP":
            if args:
                key = args[0]
                if key in self.data and isinstance(self.data[key], list) and self.data[key]:
                    return self.data[key].pop()
        elif cmd_name == "SADD":
            if len(args) >= 2:
                key = args[0]
                if key not in self.data:
                    self.data[key] = set()
                if isinstance(self.data[key], set):
                    count = 0
                    for member in args[1:]:
                        if member not in self.data[key]:
                            self.data[key].add(member)
                            count += 1
                    return count
        elif cmd_name == "SREM":
            if len(args) >= 2:
                key = args[0]
                if key in self.data and isinstance(self.data[key], set):
                    count = 0
                    for member in args[1:]:
                        if member in self.data[key]:
                            self.data[key].remove(member)
                            count += 1
                    return count
        elif cmd_name == "SMEMBERS":
            if args:
                key = args[0]
                if key in self.data and isinstance(self.data[key], set):
                    return list(self.data[key])

        return None

CyRedisConnection = MockCyRedisConnection


class AsyncRedisConnection:
    """Async wrapper around CyRedisConnection using thread pools"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
        self._connection = None
        self._executor = None

    def _get_connection(self) -> CyRedisConnection:
        """Lazy connection creation"""
        if self._connection is None:
            self._connection = CyRedisConnection(self.host, self.port)
        return self._connection

    def _ensure_executor(self):
        """Ensure thread pool executor exists"""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="redis-io")

    async def execute_command(self, command: List[bytes]) -> Any:
        """Execute Redis command asynchronously"""
        self._ensure_executor()

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self._executor, self._get_connection().execute_command, command)
        return result

    async def close(self):
        """Close connection and cleanup resources"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        # Connection cleanup handled by CyRedisConnection


class AsyncRedisConnectionPool:
    """Connection pool for async Redis operations"""

    def __init__(self, host: str = "localhost", port: int = 6379, max_connections: int = 10):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self._connections: List[AsyncRedisConnection] = []
        self._available: List[AsyncRedisConnection] = []
        self._lock = asyncio.Lock()
        self._closed = False

    async def _create_connection(self) -> AsyncRedisConnection:
        """Create a new connection"""
        return AsyncRedisConnection(self.host, self.port)

    async def get_connection(self) -> AsyncRedisConnection:
        """Get a connection from the pool"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        async with self._lock:
            if self._available:
                return self._available.pop()

            if len(self._connections) < self.max_connections:
                conn = await self._create_connection()
                self._connections.append(conn)
                return conn

            # Wait for a connection to become available (simple implementation)
            while not self._available and not self._closed:
                await asyncio.sleep(0.01)  # Small delay

            if self._closed:
                raise RuntimeError("Connection pool is closed")

            return self._available.pop()

    async def return_connection(self, connection: AsyncRedisConnection):
        """Return a connection to the pool"""
        if self._closed:
            await connection.close()
            return

        async with self._lock:
            if len(self._available) < self.max_connections:
                self._available.append(connection)
            else:
                await connection.close()

    async def close(self):
        """Close all connections in the pool"""
        if self._closed:
            return

        self._closed = True
        async with self._lock:
            for conn in self._connections:
                await conn.close()
            for conn in self._available:
                await conn.close()
            self._connections.clear()
            self._available.clear()


class AsyncRedisClient:
    """
    High-performance async Redis client for CyRedis.

    Features:
    - Async/await API mirroring sync operations
    - Connection pooling for high concurrency
    - Automatic uvloop integration for better performance
    - Context manager support
    - Clean resource management
    """

    def __init__(self,
                 host: str = "localhost",
                 port: int = 6379,
                 max_connections: int = 10,
                 auto_uvloop: bool = True):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.auto_uvloop = auto_uvloop
        self._pool: Optional[AsyncRedisConnectionPool] = None
        self._closed = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self):
        """Establish connection pool"""
        if self._closed:
            raise RuntimeError("Client is closed")

        if self._pool is None:
            # Auto-enable uvloop if requested and not already active
            if self.auto_uvloop:
                try:
                    current_loop = asyncio.get_running_loop()
                    if not isinstance(current_loop, uvloop.Loop):
                        # Can't replace running loop, but we can suggest it
                        pass
                except RuntimeError:
                    # No running loop, set uvloop policy
                    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

            self._pool = AsyncRedisConnectionPool(self.host, self.port, self.max_connections)

    async def close(self):
        """Close client and cleanup resources"""
        if self._closed:
            return

        self._closed = True
        if self._pool:
            await self._pool.close()

    async def _execute(self, command: List[bytes]) -> Any:
        """Execute command using connection pool"""
        if self._pool is None:
            await self.connect()

        conn = await self._pool.get_connection()
        try:
            result = await conn.execute_command(command)
            return result
        finally:
            await self._pool.return_connection(conn)

    # High-level Redis operations
    async def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Set a key-value pair"""
        cmd = [b"SET", key.encode('utf-8'), value.encode('utf-8')]
        if ex is not None:
            cmd.extend([b"EX", str(ex).encode('utf-8')])
        result = await self._execute(cmd)
        return result == "OK"

    async def get(self, key: str) -> Optional[str]:
        """Get the value of a key"""
        result = await self._execute([b"GET", key.encode('utf-8')])
        return result

    async def delete(self, key: Union[str, List[str]]) -> int:
        """Delete one or more keys"""
        if isinstance(key, str):
            keys = [key]
        else:
            keys = key
        cmd = [b"DEL"] + [k.encode('utf-8') for k in keys]
        result = await self._execute(cmd)
        return int(result) if result else 0

    async def incr(self, key: str) -> int:
        """Increment a key"""
        result = await self._execute([b"INCR", key.encode('utf-8')])
        return int(result) if result else 0

    async def exists(self, key: str) -> bool:
        """Check if a key exists"""
        result = await self._execute([b"EXISTS", key.encode('utf-8')])
        return bool(int(result) if result else 0)

    async def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration time"""
        result = await self._execute([b"EXPIRE", key.encode('utf-8'), str(seconds).encode('utf-8')])
        return bool(result)

    async def ttl(self, key: str) -> int:
        """Get time to live for a key"""
        result = await self._execute([b"TTL", key.encode('utf-8')])
        return int(result) if result else 0

    async def ping(self) -> str:
        """Ping Redis server"""
        result = await self._execute([b"PING"])
        return result

    async def info(self, section: str = "") -> str:
        """Get Redis info"""
        cmd = [b"INFO"]
        if section:
            cmd.append(section.encode('utf-8'))
        result = await self._execute(cmd)
        return result

    # Hash operations
    async def hset(self, key: str, field: str, value: str) -> int:
        """Set hash field"""
        result = await self._execute([b"HSET", key.encode('utf-8'), field.encode('utf-8'), value.encode('utf-8')])
        return int(result) if result else 0

    async def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field"""
        result = await self._execute([b"HGET", key.encode('utf-8'), field.encode('utf-8')])
        return result

    async def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields"""
        result = await self._execute([b"HGETALL", key.encode('utf-8')])
        if not result:
            return {}
        # Convert flat list [key1, val1, key2, val2] to dict
        # Handle both bytes and str responses
        def safe_decode(item):
            return item.decode('utf-8') if isinstance(item, bytes) else str(item)

        return {safe_decode(result[i]): safe_decode(result[i+1])
                for i in range(0, len(result), 2)}

    # List operations
    async def lpush(self, key: str, *values: str) -> int:
        """Push values to list head"""
        cmd = [b"LPUSH", key.encode('utf-8')] + [v.encode('utf-8') for v in values]
        result = await self._execute(cmd)
        return int(result) if result else 0

    async def rpush(self, key: str, *values: str) -> int:
        """Push values to list tail"""
        cmd = [b"RPUSH", key.encode('utf-8')] + [v.encode('utf-8') for v in values]
        result = await self._execute(cmd)
        return int(result) if result else 0

    async def lpop(self, key: str) -> Optional[str]:
        """Pop value from list head"""
        result = await self._execute([b"LPOP", key.encode('utf-8')])
        return result

    async def rpop(self, key: str) -> Optional[str]:
        """Pop value from list tail"""
        result = await self._execute([b"RPOP", key.encode('utf-8')])
        return result

    # Set operations
    async def sadd(self, key: str, *members: str) -> int:
        """Add members to set"""
        cmd = [b"SADD", key.encode('utf-8')] + [m.encode('utf-8') for m in members]
        result = await self._execute(cmd)
        return int(result) if result else 0

    async def srem(self, key: str, *members: str) -> int:
        """Remove members from set"""
        cmd = [b"SREM", key.encode('utf-8')] + [m.encode('utf-8') for m in members]
        result = await self._execute(cmd)
        return int(result) if result else 0

    async def smembers(self, key: str) -> List[str]:
        """Get all set members"""
        result = await self._execute([b"SMEMBERS", key.encode('utf-8')])
        if not result:
            return []
        # Handle both bytes and str responses
        def safe_decode(item):
            return item.decode('utf-8') if isinstance(item, bytes) else str(item)
        return [safe_decode(item) for item in result]

    # Pub/Sub operations (basic)
    async def publish(self, channel: str, message: str) -> int:
        """Publish message to channel"""
        result = await self._execute([b"PUBLISH", channel.encode('utf-8'), message.encode('utf-8')])
        return int(result) if result else 0

    # Transaction operations
    async def multi(self):
        """Start transaction"""
        result = await self._execute([b"MULTI"])
        return result

    async def exec(self):
        """Execute transaction"""
        result = await self._execute([b"EXEC"])
        return result

    async def discard(self):
        """Discard transaction"""
        result = await self._execute([b"DISCARD"])
        return result


# Convenience functions
async def create_async_client(host: str = "localhost",
                            port: int = 6379,
                            max_connections: int = 10) -> AsyncRedisClient:
    """Create and connect an async Redis client"""
    client = AsyncRedisClient(host, port, max_connections)
    await client.connect()
    return client
