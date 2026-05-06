# Streams and integrations

← [README](../README.md) | [Core API](core-api.md) | [Web channels](web-channels.md)

## Redis Streams

Redis Streams are an append-only log data structure. CyRedis exposes `XADD`, `XREAD`, `XREADGROUP`, `XACK`, and `XLEN` directly on `CyRedisClient`. For streaming consumption there are async iterators.

### Writing

```python
# XADD with auto-ID
entry_id = client.xadd("events", {"type": "click", "user": "u1"})

# XADD with explicit ID
entry_id = client.xadd("events", {"type": "click"}, message_id="1700000000-0")

# Async
entry_id = await client.xadd_async("events", {"type": "click"})
```

### Reading

```python
# Read up to 10 entries starting from the beginning
entries = client.xread({"events": "0-0"}, count=10, block=0)
# entries: [(stream_name, entry_id, fields_dict), ...]

# Non-blocking read for new entries only
entries = await client.xread_async({"events": "$"}, count=100, block=0)

# Consumer group
client.xreadgroup("workers", "w1", {"jobs": ">"}, count=5)
client.xack("jobs", "workers", entry_id)
```

### Stream length

```python
length = client.xlen("events")
length = await client.xlen_async("events")
```

## Async iterators

`cy_redis.utils` provides async generators that stay connected and yield messages as they arrive.

### Subscribe to a channel

```python
from cy_redis.utils import RedisPubSubIterator

async for msg in RedisPubSubIterator(client, "notifications"):
    print(msg["data"])
    # msg: {"type": "message", "channel": str, "data": str}
```

### Pattern subscribe

```python
from cy_redis.utils import RedisPSubIterator

async for msg in RedisPSubIterator(client, "events:*"):
    print(msg["pattern"], msg["channel"], msg["data"])
```

### Stream iterator

```python
from cy_redis.utils import RedisStreamIterator

# Yields entries from "logs" starting from current tail
async for stream_name, entry_id, fields in RedisStreamIterator(client, "logs"):
    print(entry_id, fields)
```

All iterators handle timeouts gracefully (yield `None` on timeout, allowing the caller to check a cancellation flag) and raise `StopAsyncIteration` on disconnection.

## ClickHouse bridge

`CyClickHouseBridge` moves data between ClickHouse and Redis using four patterns.

```python
from cy_redis.integrations.clickhouse import CyClickHouseClient, CyClickHouseBridge

ch = CyClickHouseClient(host="localhost", port=8123)
bridge = CyClickHouseBridge(ch, redis_client)
```

### Mode 1 — Live cache (ClickHouse pushes to Redis)

Auto-DDL a Redis-engine table and a MaterializedView so ClickHouse writes the latest row per key into Redis automatically after each INSERT:

```python
result = await bridge.create_live_cache(
    name="ticker_latest",
    source_query="""
        SELECT symbol, argMax(price, ts) AS last_price, max(ts) AS updated_at
        FROM ticks GROUP BY symbol
    """,
    key_column="symbol",
    redis_host="localhost",
    redis_port=6379,
    redis_key_prefix="tick:",
    refresh_interval_sec=0,       # 0 = incremental MV (22.x+)
                                  # >0 = REFRESH EVERY N SECOND (23.4+)
)
print(result["ddl"])   # inspect the generated DDL before executing
```

The Redis key for each row is `{redis_key_prefix}{key_column_value}`. Non-key columns are stored as a msgpack blob — use point-lookups only.

### Mode 2 — One-shot stream dump

Run any query and XADD all rows to a Redis Stream:

```python
count = await bridge.dump_to_stream(
    query="SELECT ts, symbol, price FROM ticks WHERE ts >= '2024-01-01' ORDER BY ts",
    stream_key="ticks:history",
    batch_size=500,
    maxlen=100_000,
)
print(f"wrote {count} rows")
```

### Mode 3 — Incremental watch

Poll ClickHouse for new rows above a watermark and XADD them as they arrive. The watermark advances automatically:

```python
async for batch_count in bridge.watch_as_stream(
    query="SELECT ts, symbol, price FROM ticks WHERE ts > '{watermark}' ORDER BY ts LIMIT 1000",
    watermark_column="ts",
    stream_key="ticks:live",
    poll_interval_sec=5.0,
    initial_watermark="2024-01-01 00:00:00",
):
    print(f"added {batch_count} rows")
```

Use `asyncio.create_task(...)` to run this in the background. Cancel the task to stop.

### Mode 4 — Channel broadcast

Consume a Redis Stream and publish each entry to a `CyChannelManager` channel so WebSocket subscribers see live events:

```python
await bridge.stream_to_channel(
    stream_key="ticks:live",
    channel="tickers",
    channel_manager=channels,   # CyChannelManager instance
    poll_interval_sec=1.0,
)
```

The last-consumed stream entry ID is persisted in `cy:ch_bridge:{stream_key}:cursor` so restarts do not replay already-delivered entries.

## Stock ticker example

The full four-mode example is in [`examples/example_clickhouse_redis.py`](../examples/example_clickhouse_redis.py). It seeds a ClickHouse `ticks` table with synthetic data and demonstrates all patterns end-to-end.

```bash
# Requires ClickHouse at localhost:8123 and Redis at localhost:6379
uv run python examples/example_clickhouse_redis.py

# With WebSocket server (mode 4):
WITH_WS=1 uv run python examples/example_clickhouse_redis.py
```

## What to read next

- [Web channels](web-channels.md) — how channel broadcast integrates with WebSocket subscribers
- [Core API](core-api.md) — raw xadd/xread signatures
- [Scripting](scripting.md) — Lua Functions used inside the channels routing
