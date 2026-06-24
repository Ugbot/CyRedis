# Web layer

← [README](../README.md) | [Web channels](web-channels.md) | [Getting started](getting-started.md)

## Web cache

`WebCache` stores HTTP responses (or arbitrary values) in Redis with automatic
expiry and a decorator for endpoints. The web layer pulls in FastAPI/PyJWT/pyotp
via the `[web]` extra (`uv pip install -e ".[web]"`).

```python
from cy_redis.web import WebCache

# WebCache(redis_client=None, backend_type="redis", prefix="cyredis:cache",
#          default_ttl=3600, backend_config=None)
cache = WebCache(redis_client, prefix="myapp", default_ttl=300)

# Manual get/set/delete — values are JSON-coded by default
cache.set("key", {"a": 1}, ttl=120)
value = cache.get("key")            # {"a": 1}
cache.delete("key")

# Bust by namespace or pattern (SCAN-based)
cache.invalidate_namespace("user")
cache.invalidate_pattern("user:*")

# Endpoint decorator (note: cached_endpoint, not cached)
@cache.cached_endpoint(ttl=60, namespace="users")
async def get_user(user_id: int) -> dict:
    return await load_user(user_id)
```

### FastAPI integration

```python
from cy_redis.web import CacheManager, create_redis_lifespan

# CacheManager wraps a backend directly; WebCache is the simpler entry point.
app = FastAPI(lifespan=create_redis_lifespan(redis_client))

cache = WebCache(redis_client)

@app.get("/users/{user_id}")
@cache.cached_endpoint(ttl=60, namespace="users")
async def get_user(user_id: int):
    ...
```

HTTP cache headers (`Cache-Control`, `ETag`) are managed by the cache's
`HTTPCacheHeaders` helper.

## Auth and session management

All auth modules live in `cy_redis.auth` and store their state in Redis. They
require the `[auth]` extra (`uv pip install -e ".[auth]"`), which pulls in
**PyJWT** (token signing/verification) and **pyotp** (TOTP 2FA).

### Token manager

Tokens are real JWTs (HS256). If you omit `secret_key`, a random one is
generated per instance.

```python
from cy_redis.auth import TokenManager

# TokenManager(redis_client, secret_key=None,
#              access_token_expiry=900, refresh_token_expiry=604800)
tokens = TokenManager(redis_client, secret_key="your-secret")

token = tokens.create_access_token("u123", claims={"role": "admin"})
payload = tokens.verify_token(token)   # dict of claims, or None if invalid/expired/revoked
tokens.revoke_token(token)
tokens.revoke_all_user_tokens("u123")

# Refresh, API, and WebSocket tokens also available:
refresh = tokens.create_refresh_token("u123")
api = tokens.create_api_token("u123", scopes=["read"])
```

### Session manager

```python
from cy_redis.auth import SessionManager

# SessionManager(redis_client, session_timeout=3600, cleanup_interval=300)
sessions = SessionManager(redis_client)

session_id = sessions.create_session("u123", {"ip": "1.2.3.4"})
session = sessions.get_session(session_id)            # dict, or None if expired
sessions.update_session(session_id, {"last_seen": "now"})
sessions.get_user_sessions("u123")                    # list of active sessions
sessions.destroy_session(session_id)                  # note: destroy_, not delete_
sessions.destroy_user_sessions("u123")
```

### WebSocket token

```python
from cy_redis.web import WebAppSupport

# WebAppSupport(redis_client=None, host="localhost", port=6379)
support = WebAppSupport(redis_client)

# Issue a token for WebSocket auth
ws_token = support.create_websocket_token("u123", permissions=["chat"])

# FastAPI dependency
async def require_ws_auth(token: str = Query(...)):
    return support.verify_user_access(token)   # claims dict, or None
```

### Two-factor auth

Backed by pyotp (RFC 6238 TOTP). `enable_2fa` returns the base32 secret, a set
of one-time backup codes, and an `otpauth://` provisioning URL for QR display.

```python
from cy_redis.auth import TwoFactorAuth

tfa = TwoFactorAuth(redis_client)

result = tfa.enable_2fa("u123")
# {'totp_secret': '...', 'backup_codes': [...], 'qr_code_url': 'otpauth://...'}

ok = tfa.verify_totp("u123", "123456")          # current 6-digit code
tfa.is_2fa_enabled("u123")                       # True
tfa.verify_backup_code("u123", "ABCD1234")       # consume a backup code
tfa.disable_2fa("u123")
```

### Password reset

```python
from cy_redis.auth import PasswordResetManager

# PasswordResetManager(redis_client, token_expiry=900)
reset = PasswordResetManager(redis_client)

token = reset.create_reset_token("u123", "user@example.com")
info = reset.verify_reset_token(token)   # {"user_id": ..., "email": ...}, or None
# Tokens are single-use: a second verify of the same token returns None.
```

## FastAPI lifespan helper

`create_redis_lifespan` wires the Redis client (and optionally a `CyChannelManager`) into the FastAPI app lifecycle:

```python
from cy_redis.web import create_redis_lifespan, get_redis, get_channels

# create_redis_lifespan(redis_client, channel_manager=None)
app = FastAPI(lifespan=create_redis_lifespan(redis_client, channel_manager))

@app.get("/info")
async def info(redis = Depends(get_redis)):
    return {"server": redis.detect_server_type(), "ping": redis.ping()}
```

The client is stored on `app.state.redis`; the channel manager on `app.state.channels`.

## What to read next

- [Web channels](web-channels.md) — distributed WebSocket pub/sub
- [Advanced features](advanced.md) — shared state, workers
- [Example](../examples/web_app_example.py) — complete web app example
- [Example](../examples/web_cache_example.py) — cache usage
