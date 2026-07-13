"""
Compatibility shim to expose CyRedisClient at the top-level path expected by tests.
"""

from cy_redis.core.cy_redis_client import (
    ConnectionError,
    CyRedisClient,
    CyRedisConnection,
    CyRedisConnectionPool,
    RedisError,
)

__all__ = [
    "CyRedisClient",
    "CyRedisConnection",
    "CyRedisConnectionPool",
    "RedisError",
    "ConnectionError",
]
