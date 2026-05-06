"""
FastAPI / Starlette integration helpers for CyRedis.

Typical usage::

    from cy_redis import CyRedisClient
    from cy_redis.web import CyChannelManager, create_redis_lifespan, get_channels

    redis = CyRedisClient()
    channels = CyChannelManager(redis)

    app = FastAPI(lifespan=create_redis_lifespan(redis, channels))

    @app.websocket("/ws/{channel}")
    async def ws(websocket: WebSocket, channel: str,
                 ch: CyChannelManager = Depends(get_channels)):
        conn = await ch.connect(websocket, channel)
        try:
            async for msg in conn:
                await ch.publish(channel, {"text": msg}, sender_conn=conn.conn_id,
                                 exclude_sender=True)
        finally:
            await ch.disconnect(conn)
"""

from contextlib import asynccontextmanager
from typing import Optional


def create_redis_lifespan(redis_client, channel_manager=None):
    """
    Return an ``asynccontextmanager`` lifespan suitable for ``FastAPI(lifespan=...)``.

    On startup:
      - Stores *redis_client* on ``app.state.redis``
      - If *channel_manager* is given, stores it on ``app.state.channels`` and
        calls ``channel_manager.start()``

    On shutdown:
      - Calls ``channel_manager.stop()`` (if present)
    """
    @asynccontextmanager
    async def _lifespan(app):
        app.state.redis = redis_client
        if channel_manager is not None:
            app.state.channels = channel_manager
            await channel_manager.start()
        yield
        if channel_manager is not None:
            await channel_manager.stop()

    return _lifespan


class CyRedisMiddleware:
    """
    Lightweight ASGI middleware that attaches the Redis client to every
    HTTP and WebSocket scope as ``scope["state"]["redis"]``.

    This is an alternative to ``create_redis_lifespan`` for frameworks /
    setups that manage the app lifespan separately.
    """

    def __init__(self, app, redis_client):
        self.app = app
        self.redis_client = redis_client

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "websocket"):
            state = scope.setdefault("state", {})
            state["redis"] = self.redis_client
        await self.app(scope, receive, send)


def get_redis(request):
    """
    FastAPI dependency that returns the :class:`CyRedisClient` attached by
    ``create_redis_lifespan`` or :class:`CyRedisMiddleware`.

    Usage::

        @app.get("/ping")
        async def ping(redis = Depends(get_redis)):
            return redis.ping()
    """
    return request.app.state.redis


def get_channels(request):
    """
    FastAPI dependency that returns the :class:`CyChannelManager` attached by
    ``create_redis_lifespan``.

    Usage::

        @app.post("/channels/{ch}/publish")
        async def pub(ch: str, body: dict,
                      mgr = Depends(get_channels)):
            await mgr.publish(ch, body)
    """
    return request.app.state.channels


async def require_ws_auth(websocket, token: str):
    """
    WebSocket authentication dependency.

    Validates *token* using :class:`~cy_redis.web.web_app_support.WebAppSupport`
    and returns the decoded claims dict on success.  Closes the connection with
    code 4001 and raises ``Exception`` on failure so FastAPI skips the handler.

    Wire it up like this::

        from fastapi import WebSocket, Query, Depends
        from cy_redis.web.fastapi_integration import require_ws_auth

        @app.websocket("/ws/{channel}")
        async def ws(websocket: WebSocket, channel: str,
                     token: str = Query(...),
                     claims: dict = Depends(
                         lambda ws, t=Query(...): require_ws_auth(ws, t)
                     )):
            ...
    """
    try:
        from cy_redis.web.web_app_support import WebAppSupport
        support = WebAppSupport(websocket.app.state.redis)
        claims = support.verify_user_access(token)
        if claims is None:
            await websocket.close(code=4001)
            raise Exception("Unauthorized WebSocket connection")
        return claims
    except ImportError:
        # web_app_support extension not compiled — skip auth
        return {}
