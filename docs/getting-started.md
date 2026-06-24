# Getting started

← [README](../README.md)

## Installation

CyRedis requires Python 3.9+ and Cython 3.0+. The hiredis C library is vendored and built automatically.

```bash
# Install in editable mode (recommended for development)
uv pip install -e .

# Build Cython extensions in-place without installing
uv run python setup.py build_ext --inplace
```

A wheel/sdist can be built with `uv build` (the package builds the vendored
hiredis automatically).

Optional feature layers — the core client has no runtime dependencies, so each
layer pulls only what it needs:

```bash
uv pip install -e ".[async]"   # uvloop for faster event loops
uv pip install -e ".[ai]"      # numpy (required for AI/vector features)
uv pip install -e ".[auth]"    # PyJWT + pyotp (tokens, sessions, 2FA)
uv pip install -e ".[web]"     # fastapi + PyJWT + pyotp (web layer)
uv pip install -e ".[game]"    # msgpack (game engine)
uv pip install -e ".[all]"     # everything above
uv pip install -e ".[dev]"     # linting, mypy, full test dependencies
```

## Connecting

```python
from cy_redis import CyRedisClient

# Defaults: localhost:6379, pool size 10
client = CyRedisClient()

# Explicit connection
client = CyRedisClient(
    host="redis.example.com",
    port=6379,
    password="secret",
    db=0,
    pool_size=20,
)
```

CyRedisClient manages a thread-safe connection pool internally. You do not need to manage connections manually.

## Sync vs async

Every command has two forms. The sync form blocks the calling thread; the async form offloads to an executor and is safe to `await` inside an asyncio event loop.

```python
# Sync (blocking)
client.set("key", "value")
value = client.get("key")

# Async (non-blocking inside asyncio)
await client.set_async("key", "value")
value = await client.get_async("key")
```

The async forms do not use a separate async connection — they call the sync implementation in `run_in_executor` so the native C connection pool is always used.

## First example

```python
import asyncio
from cy_redis import CyRedisClient

async def main():
    client = CyRedisClient(host="localhost", port=6379)

    # Basic key-value (set_async returns True on success)
    await client.set_async("hits", "0")
    client.incr("hits")                      # INCR has no async variant; it is fast and non-blocking
    print(await client.get_async("hits"))    # "1"

    # Hash
    await client.hset_async("user:1", mapping={"name": "Alice", "role": "admin"})
    print(await client.hgetall_async("user:1"))   # {"name": "Alice", "role": "admin"}

    # List
    await client.rpush_async("queue", "job1", "job2")
    print(await client.lrange_async("queue", 0, -1))   # ["job1", "job2"]

asyncio.run(main())
```

## Server type detection

CyRedis supports both Redis and Valkey (wire-compatible at RESP2/RESP3, diverging at the command level). `detect_server_type()` is a synchronous call that inspects `INFO` and returns a string — do not `await` it:

```python
server_type = client.detect_server_type()
print(server_type)   # "redis" or "valkey"
```

## What to read next

- [Core API](core-api.md) — full command reference by data type
- [Web channels](web-channels.md) — if you want WebSocket pub/sub with stream rewind
- [Streams & integrations](streams.md) — Redis Streams and the ClickHouse bridge
- [Examples](../examples/README.md) — runnable scripts for every major feature
