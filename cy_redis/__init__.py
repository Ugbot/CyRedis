"""
CyRedis - High-performance Cython Redis and Valkey client.

This package provides:
- Core Redis operations with Cython performance (hiredis, ``nogil`` I/O)
- Connection pooling, RESP2/RESP3, native TLS (hiredis_ssl/OpenSSL)
- Cluster and Sentinel clients
- Distributed locks, Lua script management and Redis Functions
- JSON, Search and Graph module wrappers with capability detection
- Redis-backed shared dictionaries and async iterators

Web/auth, worker coordination, queues, the game engine and the pgcache
server module live in the repository's ``experimental/`` tree and are not
part of this distribution.

Every public name below is backed by a compiled extension. A name whose
extension failed to import is left unbound: touching it raises an ImportError
carrying the original loader message (see ``__getattr__``), rather than handing
back a ``None`` that fails later with an unrelated TypeError. ``import_errors()``
reports everything that did not load.
"""

from typing import Any, Dict

__version__ = "0.2.0"

# name -> the ImportError raised while loading the extension behind it.
_import_errors: Dict[str, ImportError] = {}


def _unavailable(error: ImportError, *names: str) -> None:
    for name in names:
        _import_errors[name] = error


def import_errors() -> Dict[str, str]:
    """Names that failed to load, mapped to the loader's error message.

    Empty on a healthy install. A non-empty mapping means the compiled
    extensions for those names are missing or unloadable — the usual causes are
    a wheel built for a different interpreter/platform and a source install
    whose build did not finish.
    """
    return {name: str(error) for name, error in _import_errors.items()}


# Only bind what's currently built and working
try:
    from cy_redis.core.cy_redis_client import CyRedisClient
except ImportError as exc:
    _unavailable(exc, "CyRedisClient")

try:
    from cy_redis.core.cluster import CyRedisCluster
except ImportError as exc:
    _unavailable(exc, "CyRedisCluster")

try:
    from cy_redis.core.sentinel import CySentinel
except ImportError as exc:
    _unavailable(exc, "CySentinel")

try:
    from cy_redis.features.distributed import CyDistributedLock
except ImportError as exc:
    _unavailable(exc, "CyDistributedLock")

try:
    from cy_redis.data.shared_dict import CySharedDict, CySharedDictManager
except ImportError as exc:
    _unavailable(exc, "CySharedDict", "CySharedDictManager")

try:
    from cy_redis.features.script_manager import (
        CyLuaScriptManager,
        OptimizedLuaScriptManager,
    )
except ImportError as exc:
    _unavailable(exc, "CyLuaScriptManager", "OptimizedLuaScriptManager")

try:
    from cy_redis.features.functions import CyRedisFunctionsManager, RedisFunctions
except ImportError as exc:
    _unavailable(exc, "CyRedisFunctionsManager", "RedisFunctions")

try:
    from cy_redis.utils.redis_iterators import (
        RedisListIterator,
        RedisPSubIterator,
        RedisPubSubIterator,
        RedisStreamIterator,
    )
except ImportError as exc:
    _unavailable(
        exc,
        "RedisListIterator",
        "RedisPSubIterator",
        "RedisPubSubIterator",
        "RedisStreamIterator",
    )

# Make submodules available for advanced usage
try:
    from . import core
except ImportError as exc:
    _unavailable(exc, "core")

try:
    from . import features
except ImportError as exc:
    _unavailable(exc, "features")

try:
    from . import data
except ImportError as exc:
    _unavailable(exc, "data")

try:
    from . import utils
except ImportError as exc:
    _unavailable(exc, "utils")


def __getattr__(name: str) -> Any:
    error = _import_errors.get(name)
    if error is None:
        raise AttributeError(f"module 'cy_redis' has no attribute {name!r}")
    raise ImportError(
        f"cy_redis.{name} is unavailable: its compiled extension failed to "
        f"import ({error}). Reinstall cy-redis for this interpreter and "
        f"platform; `pip install --force-reinstall --no-binary cy-redis cy-redis` "
        f"rebuilds the extensions from source."
    ) from error


__all__ = [
    # Core client
    "CyRedisClient",
    # Cluster and sentinel deployments
    "CyRedisCluster",
    "CySentinel",
    # Distributed primitives
    "CyDistributedLock",
    "RedisFunctions",
    "CyRedisFunctionsManager",
    # Lua script management
    "CyLuaScriptManager",
    "OptimizedLuaScriptManager",
    # Async iterators
    "RedisStreamIterator",
    "RedisListIterator",
    "RedisPubSubIterator",
    "RedisPSubIterator",
    # Data structures
    "CySharedDict",
    "CySharedDictManager",
    # Install diagnostics
    "import_errors",
    # Submodules for advanced usage
    "core",
    "features",
    "data",
    "utils",
]
