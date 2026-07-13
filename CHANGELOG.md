# Changelog

← [README](README.md)

## [Unreleased]

## [0.2.0] - 2026-07-13

First installable, hardened release. The package now builds and installs from a
clean sdist, ships modular optional extras, and has been audited and hardened
end to end.

### Added
- **Connection AUTH + logical DB select** — `CyRedisClient(password=..., db=...)`;
  the client can now talk to a password-protected Redis/Valkey and select a DB.
- **GIL released around blocking hiredis calls** (`nogil`) — the executor-backed
  `*_async` variants now achieve real concurrency instead of serializing.
- **Modular optional extras** — `pip install cy-redis[async|ai|auth|web|game|pgcache|all]`;
  the core client has no runtime Python dependencies.
- Wider redis-py-compatible command surface: `mget`/`mset`, `type`, `rename`/`renamenx`,
  `incrby`/`decrby`/`incrbyfloat`, variadic `delete`/`exists`, `ping`, `info` (parsed),
  `xrange`/`xrevrange`, `brpoplpush`, `zrange`/`zrangebyscore` `WITHSCORES` tuples.
- Pipeline command buffering (`pipe.set(...).get(...).execute()`) and `WATCH`/`MULTI`
  optimistic-locking transactions.
- `CyChannelManager` — distributed WebSocket channels with Redis pub/sub backing, stream
  rewind, per-subscriber Lua routing filters, presence tracking, FastAPI integration.
- `RedisPSubIterator` — async generator for `PSUBSCRIBE` pattern subscriptions.
- `CyClickHouseBridge` + `CyClickHouseClient` — ClickHouse→Redis bridge.
- `detect_server_type()` — detects Redis vs Valkey at runtime.
- `docs/TIGERSTYLE.md` — the project's safety-first engineering standard.

### Fixed
- **Installable from a clean sdist**: `setup.py` builds the vendored hiredis static
  library automatically; the sdist ships hiredis sources/headers and the C++ headers;
  `readme` metadata corrected.
- **sdist no longer ships a prebuilt `libhiredis.a`** — a static archive built on the
  packaging machine is platform-specific and broke installs on every other platform
  (setup.py skipped the rebuild because the file existed). hiredis is now always
  compiled from the vendored sources.
- The cibuildwheel smoke test imports the compiled extension directly and asserts
  `CyRedisClient is not None`; the previous check passed even on a wheel whose
  extensions failed to load (`__init__` swallows `ImportError`).
- `pyproject.toml` build requirement raised to `setuptools>=77`, which the PEP 639
  `license = "MIT"` string form needs.
- Whole auth/data/workers layers were non-constructable (cdef classes with undeclared
  instance attributes) — `TokenManager`, `SessionManager`, `TwoFactorAuth`,
  `PasswordResetManager`, `ConcurrentSharedDict`, `SharedStateManager`, `WorkerQueue`,
  `MultiSessionTracker`, `WebCache` now construct.
- Connection-pool exhaustion raises `ConnectionError` instead of returning `None`.
- 2FA TOTP secrets are now valid base32 (verification previously always failed).
- LifecycleManager heartbeat now updates the status entry health checks read.
- Subpackage imports (`from cy_redis.auth import TokenManager`, etc.) now resolve.

### Security
- **pgcache module SQL injection fixed** — parameterized queries (`PQexecParams`) and
  escaped identifiers (`PQescapeIdentifier`); validated end to end.
- `PickleCoder` HMAC-signs and verifies payloads before unpickling (RCE closed);
  `JsonCoder` remains the default.
- Graph property values are escaped (Cypher injection closed).
- Password-reset tokens are single-use atomically (replay race closed) and carry a
  Redis TTL; sessions carry a Redis TTL.
- pgcache connects via `PQconnectdbParams` so the DB password is not built into a
  loggable connection string.

### Changed
- `set()` / `set_async()` return `True`/`None` (redis-py contract) instead of `"OK"`.
- Development status promoted to Beta.
- Binary wheels are now built for CPython 3.14 as well (cibuildwheel v4).
- Generated Cython C/C++ outputs are no longer tracked in git; they are regenerated
  from the `.pyx` sources on every build.
- Duplicate `RedisError`/`ConnectionError` class definitions in the core client
  collapsed to one; the client's `ConnectionError` remains a `RedisError`
  subclass (distinct from the builtin), now consistently for TLS failures too.

### Added (connection hardening + RPC)
- **Native TLS** — `CyRedisClient(use_tls=True, ssl_ca_certs=..., ssl_certfile=...,
  ssl_keyfile=..., ssl_server_name=...)`, implemented over the vendored
  hiredis_ssl and OpenSSL in a dedicated `cy_redis.core.tls_support` extension.
  Wheels always ship it; source builds without OpenSSL headers degrade to
  plain-TCP with a build warning and a descriptive runtime error if TLS is
  requested. The handshake is bounded by the connection timeout (a non-TLS
  peer that never answers the ClientHello fails fast instead of hanging the
  caller), then reads return to fully blocking so BLPOP/pub-sub waits are
  unaffected. This replaces the orphaned, never-compiled
  `cy_redis/core/connection_pool.pyx` (whose TLS class was broken and not wired
  to the hiredis client) and the pure-Python `cy_redis/connection_pool.py` shim
  that existed "to satisfy test expectations" — the real capability now lives in
  the real client, with real tests (self-signed CA + TLS redis-server spawned
  per run).
- **Connection retry with exponential backoff** — `connect_retries` /
  `connect_backoff` on connection, pool, and client. Only transient TCP
  failures retry; TLS and AUTH failures are configuration errors and fail
  immediately.
- **RPC layer compiled and completed** — `cy_redis/communication/rpc.pyx` used
  to sit in the tree uncompiled with no server half; it now ships with
  `CyRPCServer` (handler registration/decorator, BRPOP worker threads,
  heartbeat liveness), a `CyRPCClient` that awaits responses via BLPOP instead
  of polling GET, registry pruning fixes, and integration tests.

### Removed
- `cy_redis/communication/messaging_core.pyx` — an uncompilable draft
  duplicating what `cy_redis/communication/messaging.pyx` already ships (C++
  `new` under `language=c`, cimports of a nonexistent pxd, Python calls in
  `nogil` blocks, stubbed methods). Its one real feature, reliable queues,
  already lives in `messaging.pyx`.

## [0.1.0] - 2025-09-28 [UNRELEASED]

Initial development version — never published to PyPI (the sdist could not build).
Superseded by 0.2.0.

## [0.1.0] - 2025-09-28

### Added
- Initial release
- Cython Redis client built on vendored hiredis C library — no redis-py dependency
- Sync and async (`*_async`) variants for all commands
- Connection pool
- Full command coverage: strings, lists, sets, sorted sets, hashes, HyperLogLog, bitmaps, streams, pub/sub, scripting, transactions
- Redis Streams: `xadd`, `xread`, `xreadgroup`, `xack`, `xlen` + async variants
- `RedisPubSubIterator`, `RedisStreamIterator` async generators
- `WebCache` — HTTP response cache with TTL, tags, decorators
- `WebAppSupport` — JWT tokens, sessions, 2FA, password reset, WebSocket token issuance
- `SharedDict`, `ConcurrentSharedDict`, `SharedStateManager`
- Worker layer: `WorkerQueue`, `WorkerCoordinator`, `LifecycleManager`, `MultiSessionTracker`
- `CyDistributedLock` — TTL'd lock with token-checked safe release
- Advanced features: probabilistic structures (`CyBloomFilter`, `CyCountMinSketch`, `CyTopK`, `CyCuckooFilter`), JSON (`CyRedisJSON`), full-text search (`CyRedisSearch`), graph (`CyRedisGraph`), AI/tensors (`CyRedisAI`)
- Redis Functions via `CyRedisFunctionsManager` — curated built-in libraries (`cy:locks`, `cy:sema`, `cy:rate`, `cy:queue`)
- `CyLuaScriptManager` — atomic script deployment and hot-reload
- Cluster command helpers on `CyRedisClient` (`cluster_*`); no separate cluster-client class
- Reliable queue (`CyReliableQueue` in `cy_redis.communication`)
- Game engine (`cyredis_game`) — authoritative ECS simulation on Redis Streams
- PostgreSQL cache plugin (`plugins/pgcache`) — Redis module for read-through Postgres caching
- Pre-built Lua scripts: rate limiter, distributed lock, smart cache, job queue
- CI: GitHub Actions for Linux and macOS with Redis and Valkey services
