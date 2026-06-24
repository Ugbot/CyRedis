# Core API

← [README](../README.md) | [Getting started](getting-started.md)

All methods below exist on `CyRedisClient`. Many of the most common commands
also have an `*_async` coroutine variant (e.g. `set_async`, `get_async`,
`hset_async`, `rpush_async`, `xadd_async`, `zadd_async`). Not every command has
an async form — `incr`, `decr`, `incrby`, `type`, `rename`, the `cluster_*`
helpers and others are sync only. Where an async form exists it offloads the
sync implementation to an executor over the same native pool.

## Strings

```python
client.set(key, value, ex=-1, px=-1, nx=False, xx=False) -> bool   # True on success
client.get(key) -> Optional[str]
client.delete(*keys) -> int                # variadic
client.exists(*keys) -> int                # variadic; count of keys that exist
client.expire(key, seconds) -> int         # 1 if the timeout was set, else 0
client.ttl(key) -> int
client.incr(key) -> int
client.decr(key) -> int
client.incrby(key, amount) -> int
client.decrby(key, amount) -> int
client.incrbyfloat(key, amount) -> float
client.mset(mapping: dict) -> bool
client.mget(*keys) -> list
```

## Hashes

```python
client.hset(key, field=None, value=None, mapping=None) -> int
client.hget(key, field) -> Optional[str]
client.hmget(key, *fields) -> list
client.hgetall(key) -> dict          # str keys and str values
client.hdel(key, *fields) -> int
client.hexists(key, field) -> bool
client.hlen(key) -> int
client.hkeys(key) -> list
client.hvals(key) -> list
client.hincrby(key, field, amount) -> int
client.hincrbyfloat(key, field, amount) -> float
```

Pass either positional `field`/`value` or a `mapping` dict:

```python
client.hset("user:1", "name", "Alice")
client.hset("user:1", mapping={"name": "Alice", "role": "admin"})
```

## Lists

```python
client.lpush(key, *values) -> int
client.rpush(key, *values) -> int
client.lpop(key) -> Optional[str]
client.rpop(key) -> Optional[str]
client.lrange(key, start, stop) -> list
client.llen(key) -> int
client.lindex(key, index) -> Optional[str]
client.lset(key, index, value)
client.lrem(key, count, value) -> int
client.ltrim(key, start, stop)
client.rpoplpush(source, destination) -> Optional[str]
client.blpop(keys, timeout=0) -> Optional[tuple]      # keys: a list (a bare str is also accepted)
client.brpop(keys, timeout=0) -> Optional[tuple]
client.brpoplpush(source, destination, timeout=0) -> Optional[str]
```

## Sets

```python
client.sadd(key, *members) -> int
client.srem(key, *members) -> int
client.smembers(key) -> set
client.sismember(key, member) -> bool
client.scard(key) -> int
client.spop(key, count=1)
client.srandmember(key, count=1)
client.sunion(*keys) -> set
client.sinter(*keys) -> set
client.sdiff(*keys) -> set
client.sunionstore(dest, *keys) -> int
client.sinterstore(dest, *keys) -> int
client.sdiffstore(dest, *keys) -> int
client.smove(src, dst, member) -> bool
```

## Sorted sets

```python
client.zadd(key, mapping: dict, nx=False, xx=False, gt=False, lt=False) -> int
client.zrem(key, *members) -> int
client.zscore(key, member) -> Optional[float]
client.zmscore(key, *members) -> list
client.zrank(key, member) -> Optional[int]
client.zrevrank(key, member) -> Optional[int]
client.zrange(key, start, stop, withscores=False, byscore=False, bylex=False, rev=False, offset=None, count=None) -> list
client.zrevrange(key, start, stop, withscores=False) -> list
client.zrangebyscore(key, min_score, max_score, withscores=False, offset=None, count=None) -> list
client.zrevrangebyscore(key, max_score, min_score, withscores=False, offset=None, count=None) -> list
client.zcard(key) -> int
client.zcount(key, min, max) -> int
client.zincrby(key, amount, member) -> float
client.zpopmin(key, count=1) -> list
client.zpopmax(key, count=1) -> list
client.zremrangebyrank(key, start, stop) -> int
client.zremrangebyscore(key, min, max) -> int
client.zunionstore(dest, keys) -> int
client.zinterstore(dest, keys) -> int
client.zdiffstore(dest, keys) -> int
```

When `withscores=True`, results are returned as `(member, score)` tuples with
the score as a `float`:

```python
client.zrange("leaderboard", 0, -1, withscores=True)
# [("alice", 1.0), ("bob", 2.0), ("carol", 3.0)]
```

## Streams

```python
client.xadd(stream, data: dict, message_id="*") -> str
client.xread(streams: dict, count=10, block=1000) -> list[tuple]
client.xreadgroup(group, consumer, streams, count=10, block=1000, noack=False)
client.xack(stream, group, *ids) -> int
client.xlen(stream) -> int
client.xrange(stream, start="-", end="+", count=None) -> list[tuple]
client.xrevrange(stream, end="+", start="-", count=None) -> list[tuple]
client.xdel(stream, *ids) -> int
client.xtrim(stream, maxlen=None, approximate=True)
client.xpending(stream, group) -> ...
client.xclaim(stream, group, consumer, min_idle_time, *ids)
client.xgroup_create(stream, group, id="$", mkstream=False)

# Async variants
await client.xadd_async(stream, data, message_id="*") -> str
await client.xread_async(streams, count=10, block=1000) -> list[tuple]
await client.xreadgroup_async(group, consumer, streams, count=10, block=1000)
await client.xlen_async(stream) -> int
```

`xread`, `xrange`, and `xrevrange` result tuples are `(stream_name, entry_id, fields_dict)`
for `xread`, and `(entry_id, fields_dict)` for `xrange`/`xrevrange`.

For streaming consumption see [Streams & integrations](streams.md).

## Pub/Sub

```python
client.publish(channel, message) -> int
```

For subscribing, use the async iterators in `cy_redis.utils`:

```python
from cy_redis.utils import RedisPubSubIterator, RedisPSubIterator

async for msg in RedisPubSubIterator(client, "my-channel"):
    print(msg["data"])

async for msg in RedisPSubIterator(client, "events:*"):
    print(msg["channel"], msg["data"])
```

See [Streams & integrations](streams.md).

## Transactions and pipelines

`client.pipeline()` returns a `CyRedisPipeline` that buffers commands and sends
them in one batch on `execute()`. It supports `set`, `get`, `xadd`,
`execute_command`, `watch`, `multi`, `unwatch`, chaining, and the context-manager
protocol. `execute()` returns the list of replies in order:

```python
pipe = client.pipeline()
pipe.set("a", "1")
pipe.get("a")
results = pipe.execute()          # [True, "1"]

# Chaining and context manager both work
with client.pipeline() as pipe:
    results = pipe.set("b", "x").get("b").execute()   # [True, "x"]

# Optimistic locking inside a pipeline (WATCH/MULTI)
pipe = client.pipeline()
pipe.watch("counter")
pipe.multi()
pipe.set("counter", "100")
pipe.execute()
```

The client also exposes inline transaction commands (`watch`, `multi`,
`exec_`, `discard`, `unwatch`) directly on the connection:

```python
client.watch("counter")
val = client.get("counter")
client.multi()
client.set("counter", str(int(val) + 1))
client.exec_()                    # ["OK"]
```

## Key management

```python
client.type(key) -> str
client.rename(key, new_key) -> bool
client.renamenx(key, new_key) -> bool
client.expire(key, seconds) -> int
client.ttl(key) -> int
```

There is no `keys`/`scan` helper on the client. For prefix scans, issue the raw
command via `execute_command` (note: it takes a single list of arguments):

```python
cursor, batch = client.execute_command(["SCAN", "0", "MATCH", "user:*", "COUNT", "100"])
```

## Server commands

```python
client.ping() -> str                 # "PONG"
client.info(section=None) -> str     # raw INFO text
client.detect_server_type() -> str   # "redis" or "valkey" (sync, do not await)
client.execute_command(args: list)   # raw RESP command; pass a single list of arguments
```

## Bitmaps and HyperLogLog

```python
client.setbit(key, offset, value) -> int
client.getbit(key, offset) -> int
client.bitcount(key, start=None, end=None) -> int
client.bitop(operation, dest, *keys) -> int
client.bitpos(key, bit, start=None, end=None) -> int

client.pfadd(key, *elements) -> int
client.pfcount(*keys) -> int
client.pfmerge(dest, *sources)

client.bitfield(key, *args)
```

## Scripting

```python
client.eval(script, numkeys, *keys_and_args)     # returns the raw Redis reply
client.evalsha(sha, numkeys, *keys_and_args)
client.script_load(script) -> str                # returns the SHA1
client.script_exists(*shas) -> list
client.script_flush()
```

```python
sha = client.script_load("return ARGV[1]")
client.evalsha(sha, 0, "hello")        # "hello"
client.eval("return redis.call('SET', KEYS[1], ARGV[1])", 1, "k", "v")   # "OK"
```

See [Scripting](scripting.md) for the script manager and Redis Functions.

## What to read next

- [Streams & integrations](streams.md) — async iterators, ClickHouse bridge
- [Scripting](scripting.md) — Lua, Redis Functions
- [Advanced features](advanced.md) — cluster, distributed locks, shared dicts
- [Web channels](web-channels.md) — WebSocket pub/sub
