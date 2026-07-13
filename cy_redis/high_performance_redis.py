"""
Simple wrapper to provide HighPerformanceRedis expected by integration tests.
Uses the built Cython CyRedisClient for actual operations.
"""

from cy_redis.core.cy_redis_client import CyRedisClient


class HighPerformanceRedis:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        max_connections: int = 10,
        max_workers: int = 4,
        password: str = None,
        db: int = 0,
    ):
        self.client = CyRedisClient(
            host=host,
            port=port,
            max_connections=max_connections,
            max_workers=max_workers,
            password=password,
            db=db,
        )

    def keys(self, pattern: str = "*"):
        """Return keys matching pattern (KEYS)."""
        return self.client.execute_command(["KEYS", pattern])

    def close(self):
        """No-op: the underlying CyRedisClient frees its pool on GC."""
        return None

    def __getattr__(self, name):
        # Delegate any other command to the underlying CyRedisClient.
        return getattr(self.client, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
