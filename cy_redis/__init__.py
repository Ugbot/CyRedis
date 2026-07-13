"""
CyRedis - High-performance Cython Redis client with advanced features.

This package provides a comprehensive Redis client implementation with:
- Core Redis operations with Cython performance
- Connection pooling and protocol handling
- Asynchronous operations
- Distributed locks and coordination
- Reliable queues, messaging, and RPC with service discovery
- Native TLS (hiredis_ssl/OpenSSL) with connection retry/backoff
- Advanced data structures
- Web application support with authentication and sessions
- Worker coordination and lifecycle management
"""

from typing import Optional, Any

__version__ = "0.2.0"

# Only import what's currently built and working
_core_available: bool
CyRedisClient: Optional[Any]
try:
    from cy_redis.core.cy_redis_client import CyRedisClient
    _core_available = True
except ImportError:
    _core_available = False
    CyRedisClient = None

_distributed_available: bool
CyDistributedLock: Optional[Any]
try:
    from cy_redis.features.distributed import CyDistributedLock
    _distributed_available = True
except ImportError:
    _distributed_available = False
    CyDistributedLock = None

_advanced_available: bool
RedisAdvanced: Optional[Any]
try:
    from cy_redis.features.advanced import CyAdvancedRedisClient as RedisAdvanced
    _advanced_available = True
except ImportError:
    _advanced_available = False
    RedisAdvanced = None

_shared_dict_available: bool
CySharedDict: Optional[Any]
try:
    from cy_redis.data.shared_dict import CySharedDict
    _shared_dict_available = True
except ImportError:
    _shared_dict_available = False
    CySharedDict = None

_concurrent_shared_dict_available: bool
ConcurrentSharedDict: Optional[Any]
try:
    from cy_redis.data.concurrent_shared_dict import ConcurrentSharedDict
    _concurrent_shared_dict_available = True
except ImportError:
    _concurrent_shared_dict_available = False
    ConcurrentSharedDict = None

_web_cache_available: bool
WebCache: Optional[Any]
try:
    from cy_redis.web.web_cache import WebCache
    _web_cache_available = True
except ImportError:
    _web_cache_available = False
    WebCache = None

_script_manager_available: bool
CyLuaScriptManager: Optional[Any]
OptimizedLuaScriptManager: Optional[Any]
try:
    from cy_redis.features.script_manager import (
        CyLuaScriptManager,
        OptimizedLuaScriptManager,
    )
    _script_manager_available = True
except ImportError:
    _script_manager_available = False
    CyLuaScriptManager = None
    OptimizedLuaScriptManager = None

_functions_available: bool
CyRedisFunctionsManager: Optional[Any]
RedisFunctions: Optional[Any]
try:
    from cy_redis.features.functions import (
        CyRedisFunctionsManager,
        RedisFunctions,
    )
    _functions_available = True
except ImportError:
    _functions_available = False
    CyRedisFunctionsManager = None
    RedisFunctions = None

_messaging_available: bool
CyReliableQueue: Optional[Any]
ReliableQueue: Optional[Any]
try:
    from cy_redis.communication.messaging import (
        CyReliableQueue,
        ReliableQueue,
    )
    _messaging_available = True
except ImportError:
    _messaging_available = False
    CyReliableQueue = None
    ReliableQueue = None

_iterators_available: bool
RedisStreamIterator: Optional[Any]
RedisListIterator: Optional[Any]
RedisPubSubIterator: Optional[Any]
RedisPSubIterator: Optional[Any]
try:
    from cy_redis.utils.redis_iterators import (
        RedisStreamIterator,
        RedisListIterator,
        RedisPubSubIterator,
        RedisPSubIterator,
    )
    _iterators_available = True
except ImportError:
    _iterators_available = False
    RedisStreamIterator = None
    RedisListIterator = None
    RedisPubSubIterator = None
    RedisPSubIterator = None

_channels_available: bool
CyChannelManager: Optional[Any]
CyChannelConnection: Optional[Any]
try:
    from cy_redis.web.channels import CyChannelManager, CyChannelConnection
    _channels_available = True
except ImportError:
    _channels_available = False
    CyChannelManager = None
    CyChannelConnection = None

# Make submodules available for advanced usage
core: Optional[Any]
try:
    from . import core
except ImportError:
    core = None

communication: Optional[Any]
try:
    from . import communication
except ImportError:
    communication = None

features: Optional[Any]
try:
    from . import features
except ImportError:
    features = None

web: Optional[Any]
try:
    from . import web
except ImportError:
    web = None

auth: Optional[Any]
try:
    from . import auth
except ImportError:
    auth = None

workers: Optional[Any]
try:
    from . import workers
except ImportError:
    workers = None

data: Optional[Any]
try:
    from . import data
except ImportError:
    data = None

utils: Optional[Any]
try:
    from . import utils
except ImportError:
    utils = None

__all__ = [
    # Core client
    'CyRedisClient',

    # Distributed primitives
    'CyDistributedLock',
    'RedisFunctions',
    'CyRedisFunctionsManager',

    # Lua script management
    'CyLuaScriptManager',
    'OptimizedLuaScriptManager',

    # Advanced operations
    'RedisAdvanced',

    # Messaging
    'CyReliableQueue',
    'ReliableQueue',

    # Async iterators
    'RedisStreamIterator',
    'RedisListIterator',
    'RedisPubSubIterator',
    'RedisPSubIterator',

    # Channels (WebSocket + pub/sub + stream rewind)
    'CyChannelManager',
    'CyChannelConnection',

    # Data structures
    'CySharedDict',
    'ConcurrentSharedDict',

    # Web support
    'WebCache',

    # Submodules for advanced usage
    'core',
    'communication',
    'features',
    'web',
    'auth',
    'workers',
    'data',
    'utils',
]
