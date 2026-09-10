# Redis and Valkey parity

CyRedis speaks RESP2/RESP3 and talks to Redis and Valkey over the same code
path. The test suite runs in full against both (`REDIS_PORT` selects the
instance, see [testing.md](testing.md)), and `CyRedisClient.detect_server_type()`
reports `"redis"` or `"valkey"` — Valkey answers `INFO server` with a
compatibility `redis_version:` line before naming itself, so detection reads
the whole section.

## Core commands

Everything the client implements on top of the base server protocol behaves
identically on Redis 7 and Valkey 8: strings, hashes, lists, sets, sorted sets,
streams and consumer groups, pipelines and transactions, pub/sub, scripting,
Redis Functions, TTLs, ACLs, TLS, and the higher-level features built on those
(worker queues, distributed locks, shared dicts, channel manager).

## Module-backed features

Optional features come from server modules, and Redis and Valkey ship different
builds. What the server actually loaded matters more than whether it is Redis or
Valkey: plain `redis:7` and `valkey/valkey:8` carry no modules at all, while
`redis/redis-stack-server` and `valkey/valkey-bundle` do.

| Feature | Redis Stack | valkey-bundle |
| --- | --- | --- |
| `cy_redis.features.json_ops` (`JSON.*`) | RedisJSON | valkey-json, same command surface |
| `cy_redis.features.search` — `FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX` | RediSearch | valkey-search, TAG/NUMERIC/VECTOR fields |
| `cy_redis.features.search` — `FT.AGGREGATE`, `FT.SUG*`, `FT.DICT*`, `FT.ALTER`, TEXT fields | RediSearch | not implemented |
| `cy_redis.features.graph` (`GRAPH.*`) | RedisGraph, end-of-life since Redis Stack 7.4 | no equivalent |
| `cy_redis.features.ai` (`AI.*`) | RedisAI | no equivalent |

Vector search works the same on both. `ft_create` renders VECTOR fields from an
options dict and `ft_search` takes query parameters and a dialect:

```python
search.ft_create(
    "products",
    [
        ("tag", "TAG", {}),
        ("vec", "VECTOR", {"algorithm": "HNSW", "type": "FLOAT32",
                           "dim": 3, "distance_metric": "COSINE"}),
    ],
    on="HASH",
    prefix=["product:"],
)

search.ft_search(
    "products",
    "*=>[KNN 2 @vec $q AS score]",
    params={"q": struct.pack("3f", 1.0, 0.0, 0.0)},
    dialect=2,
)
```

## When a module is missing

Module commands raise `cy_redis.features.capabilities.ModuleUnavailableError`
naming the module that provides them, instead of surfacing the server's bare
`unknown command` error:

```
FT.AGGREGATE is not implemented by the module serving search indexes on this
server. valkey-search (valkey-bundle) implements only FT.CREATE, FT.DROPINDEX,
FT.INFO, FT.SEARCH, FT._LIST; this command needs RediSearch (Redis Stack).
```

Query support ahead of time with the same module:

```python
from cy_redis.features.capabilities import module_names, supports_command

conn = client.pool.get_connection()
"search" in module_names(conn)
supports_command(conn, "FT.AGGREGATE")
```
