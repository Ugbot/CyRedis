"""Advanced features for CyRedis.

Distributed locks, Lua script management, Redis Functions, probabilistic
structures, JSON, full-text search, graph, and (with the ``ai`` extra) vector
search. The vector-search layer (``CyRedisAI``) requires numpy and is guarded.
"""

from cy_redis.features.advanced import (
    CyAdvancedRedisClient,
    CyBulkOperations,
    CyCircuitBreaker,
    CyCompression,
    CyMemoryPool,
    CyMetricsCollector,
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
from cy_redis.features.probabilistic import (
    CyBloomFilter,
    CyCountMinSketch,
    CyCuckooFilter,
    CyTopK,
)
from cy_redis.features.script_manager import (
    CyLuaScriptManager,
    OptimizedLuaScriptManager,
)
from cy_redis.features.search import CyRedisSearch

try:
    from cy_redis.features.ai import CyRedisAI
except ImportError:  # optional 'ai' extra (numpy) not installed
    CyRedisAI = None

__all__ = [
    "CyDistributedLock",
    "CyReadWriteLock",
    "CyAdvancedRedisClient",
    "CyBulkOperations",
    "CyCircuitBreaker",
    "CyCompression",
    "CyMemoryPool",
    "CyMetricsCollector",
    "CyLuaScriptManager",
    "OptimizedLuaScriptManager",
    "CyRedisFunctionsManager",
    "CyLocks",
    "CyQueue",
    "CyRateLimiter",
    "RedisFunctions",
    "CyBloomFilter",
    "CyCountMinSketch",
    "CyCuckooFilter",
    "CyTopK",
    "CyRedisJSON",
    "CyRedisSearch",
    "CyRedisGraph",
    "CyRedisAI",
]
