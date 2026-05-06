# Core API

← [README](../README.md) | [Getting started](getting-started.md)

All methods below exist on `CyRedisClient`. Every method has an `*_async` coroutine variant. Only the sync signatures are shown here; append `_async` and `await` for the async form.

## Strings

```python
client.set(key, value, ex=-1, px=-1, nx=False, xx=False)
client.get(key) -> Optional[str]
client.delete(key) -> int
client.exists(key) -> bool
client.expire(key, seconds) -> bool
client.ttl(key) -> int
client.incr(key) -> int
client.decr(key) -> int
client.append(key, value) -> int
client.getset(key, value) -> Optional[str]
client.mset(mapping: dict)
client.mget(*keys) -> list
client.setnx(key, value) -> bool
client.setex(key, seconds, value)
client.getrange(key, start, end) -> str
client.strlen(key) -> int
```

## Hashes

```python
client.hset(key, field=None, value=None, mapping=None) -> int
client.hget(key, field) -> Optional[str]
client.hmget(key, *fields) -> list
client.hgetall(key) -> dict
client.hdel(key, *fields) -> int
client.hexists(key, field) -> bool
client.hlen(key) -> int
client.hkeys(key) -> list
client.hvals(key) -> list
client.hincrby(key, field, amount) -> int
client.hincrbyfloat(key, field, amount) -> float
client.hscan(key, cursor=0, match=None, count=None) -> tuple
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
client.linsert(key, where, pivot, value) -> int
client.blpop(*keys, timeout=0) -> Optional[tuple]
client.brpop(*keys, timeout=0) -> Optional[tuple]
```

## Sets

```python
client.sadd(key, *members) -> int
client.srem(key, *members) -> int
client.smembers(key) -> set
client.sismember(key, member) -> bool
client.scard(key) -> int
client.spop(key, count=None)
client.srandmember(key, count=None)
client.sunion(*keys) -> set
client.sinter(*keys) -> set
client.sdiff(*keys) -> set
client.smove(src, dst, member) -> bool
```

## Sorted sets

```python
client.zadd(key, mapping: dict, nx=False, xx=False, gt=False, lt=False) -> int
client.zrem(key, *members) -> int
client.zscore(key, member) -> Optional[float]
client.zrank(key, member) -> Optional[int]
client.zrevrank(key, member) -> Optional[int]
client.zrange(key, start, stop, withscores=False, rev=False) -> list
client.zrevrange(key, start, stop, withscores=False) -> list
client.zrangebyscore(key, min, max, withscores=False, offset=None, count=None) -> list
client.zcard(key) -> int
client.zcount(key, min, max) -> int
client.zincrby(key, amount, member) -> float
client.zpopmin(key, count=1) -> list
client.zpopmax(key, count=1) -> list
```

## Streams

```python
client.xadd(stream, data: dict, message_id="*") -> str
client.xread(streams: dict, count=10, block=1000) -> list[tuple]
client.xreadgroup(group, consumer, streams, count=10, block=1000, noack=False)
client.xack(stream, group, *ids) -> int
client.xlen(stream) -> int

# Async variants
await client.xadd_async(stream, data, message_id="*") -> str
await client.xread_async(streams, count=10, block=0) -> list[tuple]
await client.xreadgroup_async(group, consumer, streams, count=10, block=0)
```

`xread` result tuples are `(stream_name, entry_id, fields_dict)`.

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

```python
# MULTI/EXEC transaction
with client.pipeline() as pipe:
    pipe.set("a", "1")
    pipe.incr("a")
    results = pipe.execute()

# Watch for optimistic locking
client.watch("key")
val = client.get("key")
with client.pipeline(transaction=True) as pipe:
    pipe.multi()
    pipe.set("key", str(int(val) + 1))
    pipe.execute()
```

## Key management

```python
client.keys(pattern="*") -> list
client.scan(cursor=0, match=None, count=None, type=None) -> tuple
client.type(key) -> str
client.rename(key, new_key)
client.renamenx(key, new_key) -> bool
client.persist(key) -> bool
client.pttl(key) -> int
client.pexpire(key, milliseconds) -> bool
client.object_encoding(key) -> str
client.object_refcount(key) -> int
client.dump(key) -> bytes
client.restore(key, ttl, serialized_value)
client.copy(src, dst, replace=False) -> bool
```

## Server commands

```python
client.ping() -> str
client.info(section=None) -> str
client.dbsize() -> int
client.flushdb(async_=False)
client.flushall(async_=False)
client.select(db)
client.client_id() -> int
client.client_setname(name)
client.config_get(pattern) -> dict
client.config_set(name, value)
client.debug_sleep(seconds)
client.slowlog_get(count=None) -> list
client.memory_usage(key) -> int
client.latency_history(event) -> list
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
```

## Geospatial

```python
client.geoadd(key, *args) -> int
client.geodist(key, member1, member2, unit="m") -> Optional[float]
client.geopos(key, *members) -> list
client.geosearch(key, ...)
client.geosearchstore(dest, src, ...)
```

## What to read next

- [Streams & integrations](streams.md) — async iterators, ClickHouse bridge
- [Scripting](scripting.md) — Lua, Redis Functions
- [Advanced features](advanced.md) — cluster, distributed locks, shared dicts
- [Web channels](web-channels.md) — WebSocket pub/sub
