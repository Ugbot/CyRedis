"""
CyRedis - High-performance threaded Redis client using Cython and hiredis.

This package provides a high-performance Redis client with threading support,
built using Cython and the hiredis C library for optimal performance.

Includes PostgreSQL read-through caching via Redis modules.
"""

import os

try:
    from .cy_redis import CyRedisClient, ThreadedStreamManager, RedisError, ConnectionError
    from .pg_cache import (
        PostgreSQLConnection, ReadThroughCache, PGCacheManager,
        load_pgcache_module, check_pgcache_module_loaded, get_pgcache_module_info
    )
    from .redis_wrapper import (
        HighPerformanceRedis,
        ThreadedStreamConsumer,
        KeyspaceWatcher,
        KeyspaceNotificationConsumer,
        OptimisticLock,
        StreamConsumerGroup,
        WorkerQueue,
        PubSubHub,
        DistributedLock,
        ReadWriteLock,
        Semaphore,
        DistributedCounter,
        LocalCache,
        LiveObject,
        ReliableQueue,
        LuaScriptManager,
        OptimizedLuaScriptManager,
        RedisClusterManager,
        RedisSentinelManager,
        RedisScripts,
        ScriptHelper,
        WebSessionManager,
        XATransactionManager,
        ObservabilityManager,
        SecurityManager,
        PostgreSQLConnection,
        ReadThroughCache,
        PGCacheManager,
        enable_uvloop,
        is_uvloop_enabled,
        create_uvloop_event_loop,
        HighPerformanceAsyncRedis,
        RedisPluginManager,
    )
except ImportError as e:
    raise ImportError(
        "CyRedis extension not built. Please install with: pip install -e ."
    ) from e


def get_pgcache_module_path():
    """
    Get the path to the built pgcache Redis module.

    Returns:
        str: Path to pgcache.so, or None if not found
    """
    package_dir = os.path.dirname(__file__)
    module_path = os.path.join(package_dir, 'pgcache', 'pgcache.so')

    if os.path.exists(module_path):
        return module_path

    # Try to find it in the installed package
    try:
        import cy_redis.pgcache
        module_path = os.path.join(os.path.dirname(cy_redis.pgcache.__file__), 'pgcache.so')
        if os.path.exists(module_path):
            return module_path
    except ImportError:
        pass

    return None

__version__ = "0.1.0"
__all__ = [
    "CyRedisClient",
    "ThreadedStreamManager",
    "RedisError",
    "ConnectionError",
    "HighPerformanceRedis",
    "ThreadedStreamConsumer",
    "KeyspaceWatcher",
    "KeyspaceNotificationConsumer",
    "OptimisticLock",
    "StreamConsumerGroup",
    "WorkerQueue",
    "PubSubHub",
    "DistributedLock",
    "ReadWriteLock",
    "Semaphore",
    "DistributedCounter",
    "LocalCache",
    "LiveObject",
    "ReliableQueue",
    "LuaScriptManager",
    "RedisClusterManager",
    "RedisSentinelManager",
    "RedisScripts",
    "ScriptHelper",
    "WebSessionManager",
    "XATransactionManager",
    "ObservabilityManager",
    "SecurityManager",
    "PostgreSQLConnection",
    "ReadThroughCache",
    "PGCacheManager",
    "load_pgcache_module",
    "check_pgcache_module_loaded",
    "get_pgcache_module_info",
    "enable_uvloop",
    "is_uvloop_enabled",
    "create_uvloop_event_loop",
    "HighPerformanceAsyncRedis",
    "RedisPluginManager",
    "get_pgcache_module_path",
    "__version__",
]
