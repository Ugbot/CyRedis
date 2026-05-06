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

Optional extras:

```bash
uv pip install -e ".[async]"   # adds uvloop for faster event loops
uv pip install -e ".[ai]"      # adds numpy (required for AI/vector features)
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

    # Basic key-value
    await client.set_async("hits", "0")
    await client.incr_async("hits")
    print(await client.get_async("hits"))   # "1"

    # Hash
    await client.hset_async("user:1", mapping={"name": "Alice", "role": "admin"})
    print(await client.hgetall_async("user:1"))

    # List
    await client.rpush_async("queue", "job1", "job2")
    print(await client.lrange_async("queue", 0, -1))

asyncio.run(main())
```

## Server type detection

CyRedis supports both Redis and Valkey (wire-compatible at RESP2/RESP3, diverging at the command level). Detect which server you are connected to at runtime:

```python
server_type = await client.detect_server_type()
print(server_type)   # "redis" or "valkey"
```

## What to read next

- [Core API](core-api.md) — full command reference by data type
- [Web channels](web-channels.md) — if you want WebSocket pub/sub with stream rewind
- [Streams & integrations](streams.md) — Redis Streams and the ClickHouse bridge
- [Examples](../examples/README.md) — runnable scripts for every major feature
