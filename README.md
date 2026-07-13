# CyRedis

High-performance Redis client for Python, built with Cython and the vendored [hiredis](hiredis/) C library. No redis-py. No RESP parsing in Python. All connection I/O goes through native C.

## What it does

- **Full Redis command coverage** — strings, lists, sets, sorted sets, hashes, streams, HyperLogLog, bitmaps, pub/sub, scripting, transactions
- **Sync and async** — every operation has a sync path and an `*_async` coroutine; async path uses `run_in_executor` over the same native pool
- **TLS** — native via hiredis_ssl/OpenSSL, including mutual TLS and SNI; connection retry with exponential backoff built in
- **RPC** — Redis-backed request/response with service discovery, heartbeat liveness, and multi-worker servers (`cy_redis.communication.rpc`)
- **Distributed WebSocket channels** — `CyChannelManager` gives you Redis-backed pub/sub channels with stream rewind, per-subscriber filters, and presence tracking; drops into FastAPI in three lines ([docs/web-channels.md](docs/web-channels.md))
- **Web layer** — HTTP response cache, JWT tokens, session management, 2FA, password reset ([docs/web.md](docs/web.md))
- **Redis Streams** — async iterators for `SUBSCRIBE`, `PSUBSCRIBE`, and `XREAD`; ClickHouse bridge for materializing query results into streams ([docs/streams.md](docs/streams.md))
- **Lua scripting and Redis Functions** — pre-built scripts plus a script manager for atomic multi-key operations ([docs/scripting.md](docs/scripting.md))
- **Advanced features** — cluster command helpers, distributed locks, shared dicts (cross-process), probabilistic structures, JSON, full-text search, graph, RedisAI tensors/models ([docs/advanced.md](docs/advanced.md))
- **Workers** — worker queues, lifecycle manager, worker coordinator, multi-session tracker
- **Game engine** — authoritative ECS simulation backed by Redis Streams ([cyredis_game/README.md](cyredis_game/README.md))
- **PostgreSQL cache plugin** — Redis module that serves as a read-through cache for Postgres ([plugins/pgcache/README.md](plugins/pgcache/README.md))

## Quick start

```bash
pip install cy-redis
```

Binary wheels are published for CPython 3.9–3.14 on Linux (x86_64/aarch64,
glibc and musl) and macOS (x86_64/arm64). On other platforms pip builds from
the sdist, which needs a C/C++ toolchain and `make` (the vendored hiredis
builds automatically).

Optional feature layers (the core client has no runtime dependencies):

```bash
pip install "cy-redis[async]"   # uvloop
pip install "cy-redis[ai]"      # numpy (vector/AI features)
pip install "cy-redis[auth]"    # PyJWT + pyotp (tokens, 2FA)
pip install "cy-redis[web]"     # fastapi + PyJWT + pyotp
pip install "cy-redis[game]"    # msgpack (game engine)
pip install "cy-redis[all]"     # everything above
```

Working from a checkout:

```bash
# Editable install (requires Cython >= 3.0)
uv pip install -e .

# Or build extensions in-place for development
uv run python setup.py build_ext --inplace

# Build a wheel/sdist
uv build
```

### TLS

TLS rides on the vendored hiredis_ssl + OpenSSL — no Python-level socket
wrapping. PyPI wheels always ship it; source builds need OpenSSL development
headers (the build falls back to plain-TCP-only with a warning if they are
missing).

```python
from cy_redis import CyRedisClient

client = CyRedisClient(
    host="redis.example.com", port=6380,
    use_tls=True,
    ssl_ca_certs="/path/to/ca.pem",     # omit to use the system trust store
    ssl_certfile="/path/to/client.crt", # optional: mutual TLS
    ssl_keyfile="/path/to/client.key",
    ssl_server_name="redis.example.com",  # optional: SNI override
)
```

Connection establishment also retries transient TCP failures with exponential
backoff (`connect_retries=2, connect_backoff=0.1` by default); TLS and AUTH
errors are configuration problems and are never retried.

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
| [Advanced features](docs/advanced.md) | Cluster command helpers, distributed locks, shared dicts, probabilistic, JSON, search, graph |
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
  communication/  — messaging, reliable queue (CyReliableQueue)
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
