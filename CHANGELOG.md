# Changelog

← [README](README.md)

## [Unreleased]

### Added
- `CyChannelManager` — distributed WebSocket channels with Redis pub/sub backing, Ably-style stream rewind, per-subscriber Lua routing filters, presence tracking, and FastAPI lifespan integration
- `RedisPSubIterator` — async generator for `PSUBSCRIBE` pattern subscriptions
- `CyClickHouseBridge` + `CyClickHouseClient` — four-mode ClickHouse→Redis bridge: live cache (Redis engine table + MaterializedView DDL), one-shot stream dump, incremental watch loop, and channel broadcast integration
- `detect_server_type()` on `CyRedisClient` — detects Redis vs Valkey at runtime by parsing `INFO server`
- Valkey support in CI matrix (`valkey/valkey:8-alpine` on port 6380)
- hiredis vendored as fully tracked source (no git submodule) — can be patched directly

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
- `DistributedLock` (Redlock algorithm)
- Advanced features: probabilistic structures, JSON ops, full-text search, graph ops, AI/vector search
- Redis Functions: `function_load`, `fcall`, `fcall_ro`, `function_list`, `function_delete`, `function_dump`, `function_restore`
- `ScriptManager` — atomic script deployment and hot-reload
- Cluster client (`CyClusterClient`) with automatic node discovery and hash-slot routing
- Messaging and RPC (`cy_redis.communication`)
- Game engine (`cyredis_game`) — authoritative ECS simulation on Redis Streams
- PostgreSQL cache plugin (`plugins/pgcache`) — Redis module for read-through Postgres caching
- Pre-built Lua scripts: rate limiter, distributed lock, smart cache, job queue
- CI: GitHub Actions for Linux and macOS with Redis and Valkey services
