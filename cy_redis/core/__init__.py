"""
Core CyRedis Infrastructure Components.

This module contains the fundamental building blocks of CyRedis:
- Redis client implementation
- Protocol handling
- Connection pooling
- Async Redis operations
- Core Redis operations
"""

from typing import Optional, Any, Callable

# Import async client
_async_available: bool
AsyncRedisClient: Optional[Any]
create_async_client: Optional[Callable]
try:
    from .async_core import AsyncRedisClient, create_async_client
    _async_available = True
except ImportError:
    _async_available = False
    AsyncRedisClient = None
    create_async_client = None

__all__ = [
    'CyRedisClient',
    'RedisProtocol',
    'ConnectionPool',
    'AsyncRedisClient',
    'create_async_client',
    'RedisCore'
]
