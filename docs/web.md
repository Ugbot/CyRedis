# Web layer

← [README](../README.md) | [Web channels](web-channels.md) | [Getting started](getting-started.md)

## Web cache

`WebCache` stores HTTP responses (or arbitrary values) in Redis with automatic expiry, tagging, and decorator support. `CacheManager` adds cache-busting invalidation strategies.

```python
from cy_redis.web import WebCache

cache = WebCache(redis_client, default_ttl=300, key_prefix="myapp")

# Decorator
@cache.cached(ttl=60, key="user:{user_id}")
def get_user(user_id: int) -> dict:
    return db.query(user_id)

# Manual
cache.set("key", value, ttl=120)
value = cache.get("key")
cache.delete("key")
cache.invalidate_tag("user")   # bust all keys tagged "user"
```

### FastAPI integration

```python
from cy_redis.web import CacheManager, create_redis_lifespan

manager = CacheManager(redis_client)
app = FastAPI(lifespan=create_redis_lifespan(redis_client))

@app.get("/users/{id}")
@manager.cached(ttl=60)
async def get_user(id: int):
    ...
```

HTTP cache headers (`Cache-Control`, `ETag`, `Last-Modified`) are set automatically when using the decorator.

## Auth and session management

All auth modules live in `cy_redis.auth`. They store tokens and sessions in Redis and use HMAC-SHA256 signing with no third-party JWT library.

### Token manager

```python
from cy_redis.auth import TokenManager

tokens = TokenManager(redis_client, secret_key="your-secret")

token = tokens.create_token(user_id="u123", extra={"role": "admin"}, ttl=3600)
payload = tokens.verify_token(token)   # raises on invalid/expired
tokens.revoke_token(token)
tokens.revoke_all_user_tokens("u123")
```

### Session manager

```python
from cy_redis.auth import SessionManager

sessions = SessionManager(redis_client)

session_id = sessions.create_session(user_id="u123", data={"ip": "1.2.3.4"})
session = sessions.get_session(session_id)
sessions.update_session(session_id, {"last_seen": "now"})
sessions.delete_session(session_id)
sessions.get_user_sessions("u123")   # all active sessions
```

### WebSocket token

```python
from cy_redis.web import WebAppSupport

support = WebAppSupport(redis_client, secret_key="your-secret")

# Issue a short-lived token for WebSocket auth
ws_token = support.create_websocket_token(user_id="u123", ttl=30)

# FastAPI dependency
async def require_ws_auth(token: str = Query(...)):
    return support.verify_user_access(token)
```

### Two-factor auth

```python
from cy_redis.auth import TwoFactorAuth

tfa = TwoFactorAuth(redis_client)

secret = tfa.generate_secret(user_id="u123")
uri = tfa.get_provisioning_uri("u123", issuer="MyApp")
is_valid = tfa.verify_totp("u123", code="123456")
```

### Password reset

```python
from cy_redis.auth import PasswordResetManager

reset = PasswordResetManager(redis_client)

token = reset.create_reset_token(user_id="u123", ttl=3600)
user_id = reset.verify_reset_token(token)   # None if expired/invalid
reset.invalidate_token(token)
```

## FastAPI lifespan helper

`create_redis_lifespan` wires the Redis client (and optionally a `CyChannelManager`) into the FastAPI app lifecycle:

```python
from cy_redis.web import create_redis_lifespan, get_redis, get_channels

app = FastAPI(lifespan=create_redis_lifespan(redis_client, channel_manager))

@app.get("/info")
async def info(redis = Depends(get_redis)):
    return {"dbsize": await redis.dbsize_async()}
```

The client is stored on `app.state.redis`; the channel manager on `app.state.channels`.

## What to read next

- [Web channels](web-channels.md) — distributed WebSocket pub/sub
- [Advanced features](advanced.md) — shared state, workers
- [Example](../examples/web_app_example.py) — complete web app example
- [Example](../examples/web_cache_example.py) — cache usage
