"""
CyRedis - High-performance Cython Redis client with advanced features.

This package provides a comprehensive Redis client implementation with:
- Core Redis operations with Cython performance
- Connection pooling and protocol handling
- Asynchronous operations
- Distributed locks and coordination
- Messaging and RPC capabilities
- Advanced data structures
- Web application support with authentication and sessions
- Worker coordination and lifecycle management
"""

__version__ = "0.1.0"

# Only import what's currently built and working
try:
    from cy_redis.core.cy_redis_client import CyRedisClient
    _core_available = True
except ImportError:
    _core_available = False
    CyRedisClient = None

try:
    from cy_redis.features.distributed import CyDistributedLock
    _distributed_available = True
except ImportError:
    _distributed_available = False
    CyDistributedLock = None

try:
    from cy_redis.features.advanced import RedisAdvanced
    _advanced_available = True
except ImportError:
    _advanced_available = False
    RedisAdvanced = None

try:
    from cy_redis.data.shared_dict import CySharedDict
    _shared_dict_available = True
except ImportError:
    _shared_dict_available = False
    CySharedDict = None

try:
    from cy_redis.data.concurrent_shared_dict import ConcurrentSharedDict
    _concurrent_shared_dict_available = True
except ImportError:
    _concurrent_shared_dict_available = False
    ConcurrentSharedDict = None

try:
    from cy_redis.web.web_cache import WebCache
    _web_cache_available = True
except ImportError:
    _web_cache_available = False
    WebCache = None

# Make submodules available for advanced usage
try:
    from . import core
except ImportError:
    core = None

try:
    from . import communication
except ImportError:
    communication = None

try:
    from . import features
except ImportError:
    features = None

try:
    from . import web
except ImportError:
    web = None

try:
    from . import auth
except ImportError:
    auth = None

try:
    from . import workers
except ImportError:
    workers = None

try:
    from . import data
except ImportError:
    data = None

try:
    from . import utils
except ImportError:
    utils = None

__all__ = [
    # Core classes
    'CyRedisClient',
    'CyDistributedLock',
    'RedisAdvanced',

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
