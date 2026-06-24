"""
Web Integration Components for CyRedis.

This module provides:
- Distributed WebSocket channels with Redis-backed pub/sub and stream rewind
- HTTP response caching with ETag and Cache-Control support
- Web application support with authentication and session management
- FastAPI / Starlette integration helpers
"""

__all__ = [
    # Channel manager (WebSocket + pub/sub + stream rewind)
    'CyChannelManager',
    'CyChannelConnection',
    # FastAPI glue
    'create_redis_lifespan',
    'CyRedisMiddleware',
    'get_redis',
    'get_channels',
    # Caching
    'WebCache',
    'CacheManager',
    # Auth / session
    'WebAppSupport',
]

try:
    from cy_redis.web.fastapi_integration import (
        create_redis_lifespan,
        CyRedisMiddleware,
        get_redis,
        get_channels,
    )
except ImportError:  # optional 'web' extra (fastapi) not installed
    create_redis_lifespan = CyRedisMiddleware = get_redis = get_channels = None

try:
    from cy_redis.web.channels import CyChannelManager, CyChannelConnection
except ImportError:
    CyChannelManager = None
    CyChannelConnection = None

try:
    from cy_redis.web.web_cache import WebCache, CacheManager
except ImportError:
    WebCache = None
    CacheManager = None

try:
    from cy_redis.web.web_app_support import WebAppSupport
except ImportError:
    WebAppSupport = None
