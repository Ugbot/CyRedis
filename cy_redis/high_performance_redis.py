"""
Simple wrapper to provide HighPerformanceRedis expected by integration tests.
Uses the built Cython CyRedisClient for actual operations.
"""

from cy_redis.core.cy_redis_client import CyRedisClient


class HighPerformanceRedis:
    def __init__(self, host: str = "localhost", port: int = 6379, max_connections: int = 10, max_workers: int = 4):
        self.client = CyRedisClient(host=host, port=port, max_connections=max_connections, max_workers=max_workers)

    def set(self, key: str, value: str):
        return self.client.set(key, value)

    def get(self, key: str):
        return self.client.get(key)

    def delete(self, key: str):
        return self.client.delete(key)

    def publish(self, channel: str, message: str):
        return self.client.publish(channel, message)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # CyRedisClient frees resources in __dealloc__, nothing to do
        return False

