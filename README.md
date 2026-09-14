# CyRedis

High-performance Redis client for Python, built with Cython and the vendored [hiredis](https://github.com/Ugbot/CyRedis/tree/main/hiredis/) C library. No redis-py. No RESP parsing in Python. All connection I/O goes through native C.

## What it does

- **Full Redis command coverage** — strings, lists, sets, sorted sets, hashes, streams, HyperLogLog, bitmaps, pub/sub, scripting, transactions
- **Sync and async** — every operation has a sync path and an `*_async` coroutine; async path uses `run_in_executor` over the same native pool
- **TLS** — native via hiredis_ssl/OpenSSL, including mutual TLS and SNI; connection retry with exponential backoff built in
- **Cluster and Sentinel** — native `CyRedisCluster` (slot map, `MOVED`/`ASK`, cross-slot fan-out) and `CySentinel` (master discovery, failover re-resolve)
- **Redis Streams** — async iterators for `SUBSCRIBE`, `PSUBSCRIBE`, and `XREAD` ([docs/streams.md](https://github.com/Ugbot/CyRedis/blob/main/docs/streams.md))
- **Lua scripting and Redis Functions** — pre-built scripts plus a script manager for atomic multi-key operations ([docs/scripting.md](https://github.com/Ugbot/CyRedis/blob/main/docs/scripting.md))
- **Modules** — JSON, full-text/vector search and graph wrappers with capability probing, so a missing module raises a clear `ModuleUnavailableError` ([docs/advanced.md](https://github.com/Ugbot/CyRedis/blob/main/docs/advanced.md), [docs/valkey.md](https://github.com/Ugbot/CyRedis/blob/main/docs/valkey.md))
- **Distributed primitives** — distributed locks and cross-process shared dicts (`CySharedDict`)

The web layer, auth (JWT/sessions/2FA), worker coordination, RPC/reliable queues,
the ClickHouse bridge, probabilistic/AI structures, the game engine and the pgcache
Redis module are **not part of this package**. They live in
[`experimental/`](https://github.com/Ugbot/CyRedis/tree/main/experimental) in the
repository, are unsupported, and are built separately — see
[experimental/README.md](https://github.com/Ugbot/CyRedis/blob/main/experimental/README.md).

## Quick start

```bash
pip install cy-redis
```

Binary wheels are published for CPython 3.9–3.14 on Linux (x86_64/aarch64,
glibc and musl) and macOS (arm64 on 14.0+, x86_64 on 15.0+). On other platforms pip builds from
the sdist, which needs a C/C++ toolchain and `make` (the vendored hiredis
builds automatically).

The core client has no runtime dependencies. One optional extra:

```bash
pip install "cy-redis[async]"   # uvloop
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

See [docs/getting-started.md](https://github.com/Ugbot/CyRedis/blob/main/docs/getting-started.md) for connection options, pooling, and the first 10 minutes.

## Documentation

| Page | What it covers |
|------|---------------|
| [Getting started](https://github.com/Ugbot/CyRedis/blob/main/docs/getting-started.md) | Install, connect, sync vs async, connection pool |
| [Core API](https://github.com/Ugbot/CyRedis/blob/main/docs/core-api.md) | Commands by data type, transactions, pipelines |
| [Streams & integrations](https://github.com/Ugbot/CyRedis/blob/main/docs/streams.md) | Redis Streams, async iterators (ClickHouse bridge is experimental) |
| [Scripting](https://github.com/Ugbot/CyRedis/blob/main/docs/scripting.md) | Lua scripts, Redis Functions, script manager |
| [Advanced features](https://github.com/Ugbot/CyRedis/blob/main/docs/advanced.md) | Cluster command helpers, distributed locks, shared dicts, JSON, search, graph |
| [Redis and Valkey parity](https://github.com/Ugbot/CyRedis/blob/main/docs/valkey.md) | What behaves identically, and which module features differ |
| [Testing](https://github.com/Ugbot/CyRedis/blob/main/docs/testing.md) | Running the test suite, CI, adding tests |
| [Examples](https://github.com/Ugbot/CyRedis/blob/main/examples/README.md) | Runnable example scripts |
| [Experimental](https://github.com/Ugbot/CyRedis/blob/main/experimental/README.md) | Unsupported subsystems outside the wheel: web/auth, workers, queues, game engine, pgcache |
| [Changelog](https://github.com/Ugbot/CyRedis/blob/main/CHANGELOG.md) | Version history |

## Architecture

```
cy_redis/                      — the published package
  core/           — CyRedisClient, connection pool, protocol negotiation,
                    cluster, sentinel, TLS, async core
  features/       — distributed locks, functions, script_manager,
                    capabilities, json_ops, search, graph
  data/           — shared_dict (CySharedDict, CySharedDictManager)
  lua_scripts/    — bundled Lua sources
  utils/          — redis_iterators (stream/pubsub async generators)
hiredis/                       — vendored hiredis C library (built into every extension)
experimental/                  — unsupported, not packaged; separate build
  cyredis_experimental/
    auth/ web/ workers/ communication/ game/ extras/
  pgcache/        — PostgreSQL read-through cache Redis module
  tests/
```

## Requirements

- Python 3.9+
- Cython >= 3.0
- A running Redis or Valkey instance

No runtime Python dependencies. All Redis communication goes through the vendored hiredis C library compiled into each extension.

## License

MIT
