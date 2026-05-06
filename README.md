# CyRedis

High-performance Redis client for Python, built with Cython and the vendored [hiredis](hiredis/) C library. No redis-py. No RESP parsing in Python. All connection I/O goes through native C.

## What it does

- **Full Redis command coverage** — strings, lists, sets, sorted sets, hashes, streams, HyperLogLog, bitmaps, pub/sub, scripting, transactions
- **Sync and async** — every operation has a sync path and an `*_async` coroutine; async path uses `run_in_executor` over the same native pool
- **Distributed WebSocket channels** — `CyChannelManager` gives you Redis-backed pub/sub channels with stream rewind, per-subscriber filters, and presence tracking; drops into FastAPI in three lines ([docs/web-channels.md](docs/web-channels.md))
- **Web layer** — HTTP response cache, JWT tokens, session management, 2FA, password reset ([docs/web.md](docs/web.md))
- **Redis Streams** — async iterators for `SUBSCRIBE`, `PSUBSCRIBE`, and `XREAD`; ClickHouse bridge for materializing query results into streams ([docs/streams.md](docs/streams.md))
- **Lua scripting and Redis Functions** — pre-built scripts plus a script manager for atomic multi-key operations ([docs/scripting.md](docs/scripting.md))
- **Advanced features** — cluster client, distributed locks, shared dicts (cross-process), probabilistic structures, JSON, vector search, graph ([docs/advanced.md](docs/advanced.md))
- **Workers** — worker queues, lifecycle manager, worker coordinator, multi-session tracker
- **Game engine** — authoritative ECS simulation backed by Redis Streams ([cyredis_game/README.md](cyredis_game/README.md))
- **PostgreSQL cache plugin** — Redis module that serves as a read-through cache for Postgres ([plugins/pgcache/README.md](plugins/pgcache/README.md))

## Quick start

```bash
# Build (requires Cython >= 3.0)
uv pip install -e .

# Or build extensions in-place for development
uv run python setup.py build_ext --inplace
```

```python
from cy_redis import CyRedisClient

client = CyRedisClient(host="localhost", port=6379)

client.set("greeting", "hello")
print(client.get("greeting"))   # "hello"

# Async
import asyncio
async def main():
    await client.set_async("key", "value")
    print(await client.get_async("key"))
asyncio.run(main())
```

See [docs/getting-started.md](docs/getting-started.md) for connection options, pooling, and the first 10 minutes.

## Documentation

| Page | What it covers |
|------|---------------|
| [Getting started](docs/getting-started.md) | Install, connect, sync vs async, connection pool |
| [Core API](docs/core-api.md) | Commands by data type, transactions, pipelines |
| [Web channels](docs/web-channels.md) | `CyChannelManager` — WebSocket pub/sub, stream rewind, filters, presence |
| [Web layer](docs/web.md) | Web cache, JWT, sessions, 2FA, FastAPI integration |
| [Streams & integrations](docs/streams.md) | Redis Streams, async iterators, ClickHouse bridge |
| [Scripting](docs/scripting.md) | Lua scripts, Redis Functions, script manager |
| [Advanced features](docs/advanced.md) | Cluster, distributed locks, shared dicts, probabilistic, JSON, search, graph |
| [Testing](docs/testing.md) | Running the test suite, CI, adding tests |
| [Examples](examples/README.md) | Runnable example scripts |
| [Plugins](plugins/README.md) | Plugin architecture, pgcache |
| [Game engine](cyredis_game/README.md) | ECS game engine on Redis |
| [Changelog](CHANGELOG.md) | Version history |

## Architecture

```
cy_redis/
  core/           — CyRedisClient, connection pool, protocol, async core
  features/       — advanced: distributed, functions, script_manager,
                    probabilistic, json_ops, search, graph, ai
  auth/           — token_manager, session_manager, two_factor_auth,
                    password_reset_manager
  data/           — shared_dict, concurrent_shared_dict, shared_state_manager
  workers/        — worker_queue, lifecycle_manager, worker_coordinator,
                    multi_session_tracker
  communication/  — messaging, rpc
  web/            — web_cache, web_app_support, channels, fastapi_integration
  utils/          — redis_iterators (stream/pubsub async generators)
  integrations/   — clickhouse bridge
hiredis/          — vendored hiredis C library (built into every extension)
cyredis_game/     — game engine extension
plugins/          — loadable Redis server modules
  pgcache/        — PostgreSQL read-through cache module
```

## Requirements

- Python 3.9+
- Cython >= 3.0
- A running Redis or Valkey instance

No runtime Python dependencies. All Redis communication goes through the vendored hiredis C library compiled into each extension.

## License

MIT
