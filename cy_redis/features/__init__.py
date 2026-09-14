"""Feature layers built on the core client.

Distributed locks, Lua script management, Redis Functions, server capability
probing, and the JSON, full-text search and graph module wrappers.
"""

from cy_redis.features.capabilities import (
    ModuleUnavailableError,
    module_names,
    supports_command,
)
from cy_redis.features.distributed import CyDistributedLock, CyReadWriteLock
from cy_redis.features.functions import (
    CyLocks,
    CyQueue,
    CyRateLimiter,
    CyRedisFunctionsManager,
    RedisFunctions,
)
from cy_redis.features.graph import CyRedisGraph
from cy_redis.features.json_ops import CyRedisJSON
from cy_redis.features.script_manager import (
    CyLuaScriptManager,
    OptimizedLuaScriptManager,
)
from cy_redis.features.search import CyRedisSearch

__all__ = [
    "CyDistributedLock",
    "CyReadWriteLock",
    "CyLuaScriptManager",
    "OptimizedLuaScriptManager",
    "CyRedisFunctionsManager",
    "CyLocks",
    "CyQueue",
    "CyRateLimiter",
    "RedisFunctions",
    "CyRedisJSON",
    "CyRedisSearch",
    "CyRedisGraph",
    "ModuleUnavailableError",
    "module_names",
    "supports_command",
]
