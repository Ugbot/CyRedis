# Advanced features

← [README](../README.md) | [Core API](core-api.md) | [Scripting](scripting.md)

## Cluster mode

`CyClusterClient` extends `CyRedisClient` with automatic node discovery, intelligent key routing (hash-slot calculation), and cluster-aware multi-key operations.

```python
from cy_redis import CyClusterClient

cluster = CyClusterClient(
    startup_nodes=[
        {"host": "node1", "port": 7000},
        {"host": "node2", "port": 7001},
    ]
)

# Keys are automatically routed to the correct node
cluster.set("user:1", "alice")
cluster.get("user:1")

# Hash tags force keys to the same slot
cluster.set("{user:1}.name", "alice")
cluster.set("{user:1}.email", "alice@example.com")

# Cross-slot multi-key operations fan out across nodes
values = cluster.mget("user:1", "user:2", "user:3")
```

### Cluster management

```python
cluster.cluster_info()           # cluster topology and health
cluster.cluster_nodes()          # all nodes and their roles
cluster.cluster_keyslot(key)     # which slot a key belongs to
cluster.cluster_countkeysinslot(slot)
cluster.cluster_myid()           # this node's ID

# Node-specific operations
cluster.cluster_meet(host, port)
cluster.cluster_forget(node_id)
cluster.cluster_replicate(node_id)
cluster.cluster_failover()
cluster.cluster_reset(hard=False)
```

### Cluster-safe scripting

In cluster mode all keys in a EVAL/FCALL call must map to the same hash slot. Use hash tags to force co-location:

```python
# All three keys land in the same slot
client.eval(script, 3, "{u:1}:profile", "{u:1}:sessions", "{u:1}:tokens")
```

## Distributed locks

`DistributedLock` in `cy_redis.features.distributed` implements the Redlock algorithm with automatic expiry and safe release via a Lua script.

```python
from cy_redis.features import DistributedLock

lock = DistributedLock(client, name="billing:job", ttl=30)

async with lock:
    # Only one process runs this at a time
    process_billing()

# Manual acquire/release
acquired = await lock.acquire(timeout=5.0)
if acquired:
    try:
        ...
    finally:
        await lock.release()
```

The release script checks that the token matches before deleting, preventing accidental release of a lock held by another process after expiry.

## Shared dictionaries

Shared dicts expose a Python dict-like interface backed by a Redis Hash, making data accessible to multiple processes without explicit serialization.

```python
from cy_redis.data import SharedDict, ConcurrentSharedDict

# SharedDict — simple, no locking
shared = SharedDict(client, name="config")
shared["timeout"] = "30"
shared["retries"] = "3"
print(shared["timeout"])
del shared["retries"]

# ConcurrentSharedDict — optimistic locking for concurrent writes
csd = ConcurrentSharedDict(client, name="counters")
csd.increment("page_views", 1)
csd.compare_and_set("status", expected="idle", new="running")
```

### Shared state manager

`SharedStateManager` wraps multiple shared dicts and adds pub/sub change notifications so processes can react to state changes in real time:

```python
from cy_redis.data import SharedStateManager

state = SharedStateManager(client, namespace="app")
state.set("feature_flags", {"dark_mode": True, "beta": False})

# Subscribe to changes
async for change in state.watch("feature_flags"):
    print(change)
```

## Probabilistic structures

```python
from cy_redis.features import ProbabilisticStructures

prob = ProbabilisticStructures(client)

# Bloom filter (approximate membership)
prob.bf_add("visited_urls", "https://example.com")
prob.bf_exists("visited_urls", "https://example.com")   # True

# Count-Min sketch (frequency estimation)
prob.cms_add("word_freq", "redis")
prob.cms_query("word_freq", "redis")   # estimated count

# Top-K (heavy hitters)
prob.topk_add("popular", "page_a", "page_b")
prob.topk_list("popular")

# Cuckoo filter (delete-supporting Bloom filter)
prob.cf_add("blacklist", "bad_token")
prob.cf_del("blacklist", "bad_token")
prob.cf_exists("blacklist", "bad_token")
```

## JSON operations

Requires RedisJSON module on the server.

```python
from cy_redis.features import JsonOps

json_ops = JsonOps(client)

json_ops.json_set("doc:1", "$", {"name": "Alice", "scores": [10, 20, 30]})
doc = json_ops.json_get("doc:1", "$")
json_ops.json_arrappend("doc:1", "$.scores", 40)
json_ops.json_numincrby("doc:1", "$.scores[0]", 5)
json_ops.json_del("doc:1", "$.name")
```

## Full-text search

Requires RediSearch module (or Redis Stack).

```python
from cy_redis.features import SearchOps

search = SearchOps(client)

search.ft_create("idx:users",
    schema={"name": "TEXT", "age": "NUMERIC", "city": "TAG"})

search.ft_add("idx:users", "user:1",
    fields={"name": "Alice Smith", "age": "30", "city": "London"})

results = search.ft_search("idx:users", "Alice @city:{London}")
results = search.ft_search("idx:users", "@age:[25 35]")
```

## Graph operations

Requires RedisGraph module.

```python
from cy_redis.features import GraphOps

graph = GraphOps(client)

graph.graph_query("social", "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})")
results = graph.graph_query("social", "MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name")
```

## AI / vector search

Requires numpy (`uv pip install -e ".[ai]"`).

```python
from cy_redis.features import AIVectorSearch
import numpy as np

ai = AIVectorSearch(client)

# Store a vector
embedding = np.array([0.1, 0.2, 0.3], dtype=np.float32)
ai.store_vector("vec:1", embedding)

# Nearest-neighbor search
results = ai.search_similar("vec:1", k=10)
```

## Workers

```python
from cy_redis.workers import WorkerQueue, WorkerCoordinator, LifecycleManager

# Enqueue work
queue = WorkerQueue(client, name="jobs")
queue.enqueue({"type": "email", "to": "user@example.com"})

# Process work
job = queue.dequeue(block=True, timeout=5)
queue.ack(job["id"])

# Coordinate across workers
coord = WorkerCoordinator(client, worker_id="w1")
coord.heartbeat()
coord.claim_partition(partition_id="shard-0")

# Lifecycle (graceful shutdown)
lifecycle = LifecycleManager(client, service="api-server")
lifecycle.register()
lifecycle.signal_shutdown("api-server-2")
```

## Messaging and RPC

```python
from cy_redis.communication import Messaging, RPC

msg = Messaging(client)
msg.send("inbox:u1", {"type": "notification", "text": "hello"})
messages = msg.receive("inbox:u1", count=10)

# Request-reply RPC
rpc = RPC(client)
response = await rpc.call("math.add", args=[1, 2], timeout=5.0)
```

## What to read next

- [Scripting](scripting.md) — Lua and Functions underpin distributed locks
- [Web channels](web-channels.md) — shared dict pattern for subscription state
- [Testing](testing.md) — how to test against a real Redis instance
