# Advanced features

← [README](../README.md) | [Core API](core-api.md) | [Scripting](scripting.md)

## Cluster support

There is no separate cluster-client class. `CyRedisClient` exposes the Redis
Cluster administration and routing commands directly as `cluster_*` helper
methods. They require the server to be running in cluster mode (a single,
non-clustered instance returns `ERR This instance has cluster support disabled`).

```python
from cy_redis import CyRedisClient

client = CyRedisClient(host="node1", port=7000)

# Topology and slot inspection
client.cluster_info()                      # cluster topology and health
client.cluster_nodes()                     # all nodes and their roles
client.cluster_shards()
client.cluster_slots()
client.cluster_keyslot("user:1")           # which slot a key belongs to
client.cluster_countkeysinslot(slot)
client.cluster_myid()                      # this node's ID

# Node-specific administration
client.cluster_meet(host, port)
client.cluster_forget(node_id)
client.cluster_replicate(node_id)
client.cluster_failover()
client.cluster_reset(hard=False)
client.cluster_addslots(...)
client.cluster_delslots(...)
client.cluster_setslot(...)
```

`cluster_keyslot` is useful for verifying hash-tag co-location:

```python
client.cluster_keyslot("{user:1}.name")    # same slot as ...
client.cluster_keyslot("{user:1}.email")   # ... this key
```

### Cluster-aware multi-key helpers

```python
client.cluster_multi_get(keys)             # group keys by slot, fan out GETs
client.cluster_multi_set(mapping)          # group writes by slot
client.cluster_aware_execute(...)          # route a command by its key's slot
client.cluster_health_check()
```

### Cluster-safe scripting

In cluster mode all keys in an EVAL/EVALSHA call must map to the same hash slot.
Use hash tags to force co-location:

```python
# All three keys land in the same slot
client.eval(script, 3, "{u:1}:profile", "{u:1}:sessions", "{u:1}:tokens")
```

## Distributed locks

`CyDistributedLock` (in `cy_redis.features`) implements a TTL'd lock with safe
release via a Lua script that verifies the caller still owns the lock token.

```python
from cy_redis.features import CyDistributedLock

# Positional lock_key; ttl is in milliseconds
lock = CyDistributedLock(client, "billing:job", ttl_ms=30000)

# Blocking acquire (default) or non-blocking
if lock.try_acquire(blocking=False):
    try:
        process_billing()       # only one process runs this at a time
    finally:
        lock.release()          # returns None; no-op if not held

# Inspect / extend an held lock
lock.is_locked()                # bool
lock.get_ttl()                  # remaining ms, or None
lock.extend(10000)              # add 10s; True if the lock was still ours

# Async forms also exist
await lock.try_acquire_async()
await lock.release_async()
await lock.extend_async(10000)
```

Constructor: `CyDistributedLock(redis_client, lock_key, lock_value=None,
ttl_ms=30000, retry_delay=0.1, max_retries=50)`. The release script checks the
token matches before deleting, preventing accidental release of a lock held by
another process after expiry.

## Shared dictionaries

Shared dicts expose a Python dict-like interface backed by a Redis Hash, making data accessible to multiple processes without explicit serialization.

```python
from cy_redis.data import CySharedDict, ConcurrentSharedDict

# CySharedDict(redis_client, dict_key) — dict_key is positional
shared = CySharedDict(client, "config")
shared["timeout"] = "30"
shared["retries"] = "3"
print(shared.get("timeout"))
shared.increment("hits", 1)        # atomic numeric increment
shared.increment_float("rate", 0.5)
print(shared.keys(), shared.items())

# ConcurrentSharedDict(dict_name, redis_client) — note argument order
csd = ConcurrentSharedDict("counters", client)
csd.increment("page_views", 1)
csd.multi_set({"a": "1", "b": "2"})
print(csd.multi_get("a", "b"))       # variadic -> {"a": "1", "b": "2"}
print(csd.bulk_get(["a", "b"]))      # list arg -> ["1", "2"]
```

### Shared state manager

`SharedStateManager(redis_client)` provides distributed locks, counters, shared
data, and pub/sub events for coordinating worker processes:

```python
from cy_redis.data import SharedStateManager

state = SharedStateManager(client)

# Shared data
state.set_shared_data("feature_flags", {"dark_mode": True, "beta": False})
print(state.get_shared_data("feature_flags"))

# Counters
state.increment_counter("views", 1)
print(state.get_counter("views"))

# Distributed locks
token = state.acquire_lock("job", timeout=30)
if token:
    try:
        ...
    finally:
        state.release_lock("job", token)

# Pub/sub events
state.publish_event("topic", {"kind": "update"})
```

## Probabilistic structures

These are self-contained, in-process Cython data structures (not Redis
server-side modules). Each is constructed standalone — no client argument.

```python
from cy_redis.features import (
    CyBloomFilter, CyCountMinSketch, CyTopK, CyCuckooFilter,
)

# Bloom filter (approximate membership)
bf = CyBloomFilter(capacity=10000, false_positive_rate=0.01)
bf.add("https://example.com")
bf.contains("https://example.com")   # True
bf.contains("https://other.com")     # probably False

# Count-Min sketch (frequency estimation)
cms = CyCountMinSketch(epsilon=0.01, delta=0.01)
cms.add("redis")
cms.add("redis")
cms.estimate("redis")                # ~2

# Top-K (heavy hitters)
tk = CyTopK(k=10)
tk.add("page_a")
tk.add("page_b")
tk.get_top_k()                       # [("page_a", count), ...]

# Cuckoo filter (membership with deletion)
cf = CyCuckooFilter(capacity=10000)
cf.add("bad_token")
cf.contains("bad_token")             # True
cf.remove("bad_token")
cf.contains("bad_token")             # False
```

These feature classes manage their **own** connection pool — construct them with
`host`/`port` (and optional `password`, `db`), not with an existing
`CyRedisClient`. Each requires the corresponding server-side module to be loaded.

## JSON operations

Requires the RedisJSON module on the server.

```python
from cy_redis.features import CyRedisJSON

json_ops = CyRedisJSON(host="localhost", port=6379)

json_ops.json_set("doc:1", "$", {"name": "Alice", "scores": [10, 20, 30]})
doc = json_ops.json_get("doc:1", "$")
json_ops.json_arrappend("doc:1", "$.scores", 40)
json_ops.json_numincrby("doc:1", "$.scores[0]", 5)
json_ops.json_del("doc:1", "$.name")
```

## Full-text search

Requires the RediSearch module (or Redis Stack).

```python
from cy_redis.features import CyRedisSearch

search = CyRedisSearch(host="localhost", port=6379)

# schema is a list of (field_name, field_type, options) tuples
search.ft_create("idx:users",
    schema=[("name", "TEXT", {}), ("age", "NUMERIC", {}), ("city", "TAG", {})],
    on="HASH", prefix=["user:"])

# Index documents by writing the underlying HASH, then query
search.ft_search("idx:users", "Alice @city:{London}")
search.ft_search("idx:users", "@age:[25 35]")
```

## Graph operations

Requires the RedisGraph module.

```python
from cy_redis.features import CyRedisGraph

graph = CyRedisGraph(host="localhost", port=6379)

graph.query("social", "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})")
results = graph.query("social", "MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name")
# Convenience helpers: create_node, create_edge, get_neighbors, find_shortest_path
```

## AI / tensors and models

Requires the RedisAI module. Construct with `host`/`port`.

```python
from cy_redis.features import CyRedisAI

ai = CyRedisAI(host="localhost", port=6379)

# Tensors
ai.tensorset("t1", "FLOAT", [2, 2], values=[1.0, 2.0, 3.0, 4.0])
ai.tensorget("t1")

# Models
ai.modelstore("m1", backend="TORCH", device="CPU", data=model_bytes,
              inputs=["a", "b"], outputs=["c"])
ai.modelexecute("m1", inputs=["a", "b"], outputs=["c"])
```

## Workers

```python
from cy_redis.workers import WorkerQueue, WorkerCoordinator, LifecycleManager

# Enqueue work — note the constructor is WorkerQueue(queue_name, redis_client)
queue = WorkerQueue("jobs", client, max_workers=4)
task_id = queue.enqueue({"type": "email", "to": "user@example.com"})
print(queue.get_queue_stats())     # {'queue_length': 1, 'processing_count': 0, ...}

# Coordinate across workers
coord = WorkerCoordinator(client, "w1")
coord.register_worker("w1", {"host": "10.0.0.1", "started": "..."})
coord.get_all_workers()
coord.get_healthy_workers()
coord.detect_dead_workers()

# Lifecycle (startup/shutdown hooks, health)
lifecycle = LifecycleManager(client, "api-server")
lifecycle.add_startup_hook(lambda: print("starting"))
lifecycle.add_shutdown_hook(lambda: print("stopping"))
lifecycle.initialize()
lifecycle.is_healthy()             # True
lifecycle.shutdown(graceful=True)
```

## Reliable queue

`cy_redis.communication` provides a reliable, at-least-once queue with
visibility timeouts, retries, and a dead-letter queue. Use `CyReliableQueue`
directly with a `CyRedisClient` (the `ReliableQueue` Python wrapper expects a
different client object). There is no separate `Messaging`/`RPC` class.

```python
from cy_redis.communication import CyReliableQueue

q = CyReliableQueue(client, "tasks", visibility_timeout=30, max_retries=3)

msg_id = q.push({"job": "resize", "id": 42}, priority=0, delay=0)
items = q.pop(1)                 # [(msg_id, {"job": "resize", "id": 42})]
for mid, payload in items:
    q.ack(mid)                   # or q.nack(mid, retry=True)
print(q.get_stats())             # {'pending': 0, 'processing': 0, 'failed': 0, ...}
```

## What to read next

- [Scripting](scripting.md) — Lua and Functions underpin distributed locks
- [Web channels](web-channels.md) — shared dict pattern for subscription state
- [Testing](testing.md) — how to test against a real Redis instance
