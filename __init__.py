"""
CyRedis - High-performance threaded Redis client using Cython and hiredis.

This package provides a high-performance Redis client with threading support,
built using Cython and the hiredis C library for optimal performance.

Includes PostgreSQL read-through caching via Redis modules.
"""

from typing import Any, Optional
import os

from cy_redis.core.cy_redis_client import (
    CyRedisClient,
    CyRedisConnection,
    CyRedisConnectionPool,
    RedisError,
    ConnectionError,
)
from cy_redis.high_performance_redis import HighPerformanceRedis
from cy_redis.distributed import CyDistributedLock, CyReadWriteLock

try:
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
except ImportError:
    # Some optional modules may not be built; keep core usable.
    HighPerformanceRedis = None
    ThreadedStreamConsumer = None
    KeyspaceWatcher = None
    KeyspaceNotificationConsumer = None
    OptimisticLock = None
    StreamConsumerGroup = None
    WorkerQueue = None
    PubSubHub = None
    DistributedLock = None
    ReadWriteLock = None
    Semaphore = None
    DistributedCounter = None
    LocalCache = None
    LiveObject = None
    ReliableQueue = None
    LuaScriptManager = None
    OptimizedLuaScriptManager = None
    RedisClusterManager = None
    RedisSentinelManager = None
    RedisScripts = None
    ScriptHelper = None
    WebSessionManager = None
    XATransactionManager = None
    ObservabilityManager = None
    SecurityManager = None
    PostgreSQLConnection = None
    ReadThroughCache = None
    PGCacheManager = None
    enable_uvloop = None
    is_uvloop_enabled = None
    create_uvloop_event_loop = None
    HighPerformanceAsyncRedis = None
    RedisPluginManager = None


def get_pgcache_module_path() -> Optional[str]:
    """
    Get the path to the built pgcache Redis module.

    Returns:
        str: Path to pgcache.so, or None if not found
    """
    package_dir = os.path.dirname(__file__)
    module_path = os.path.join(package_dir, 'plugins', 'pgcache', 'pgcache.so')

    if os.path.exists(module_path):
        return module_path

    # Try to find it in the installed package
    try:
        import plugins.pgcache
        module_path = os.path.join(os.path.dirname(plugins.pgcache.__file__), 'pgcache.so')
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
