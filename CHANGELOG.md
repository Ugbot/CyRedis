# Changelog

← [README](README.md)

## [Unreleased]

## [0.2.0] - 2026-06-24

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
