# Web channels

← [README](../README.md) | [Web layer](web.md) | [Streams & integrations](streams.md)

`CyChannelManager` provides distributed WebSocket channels backed by Redis pub/sub and Redis Streams. Multiple Python processes share the same channel state through Redis — no separate message broker needed.

## Why not Socket.IO

| | Socket.IO | CyChannelManager |
|---|---|---|
| Distribution | Adapter required (redis-adapter) | Native — Redis IS the transport |
| Language | JS SDK coupling | Server-side Python only |
| History / rewind | Not built in | Redis Stream, N-message or cursor resume |
| Per-subscriber filtering | Client-side only | Server-side Lua, zero Python hot path |
| Presence | Polling | Cross-server Redis HASH |

## FastAPI setup

```python
from cy_redis import CyRedisClient
from cy_redis.web import CyChannelManager, create_redis_lifespan, get_channels
from fastapi import FastAPI, WebSocket, Depends, Query

redis = CyRedisClient(host="localhost", port=6379)
channels = CyChannelManager(redis, stream_maxlen=1000)

app = FastAPI(lifespan=create_redis_lifespan(redis, channels))

@app.websocket("/ws/{channel}")
async def ws(
    websocket: WebSocket,
    channel: str,
    rewind: int = Query(None),
    since: str = Query(None),
    ch: CyChannelManager = Depends(get_channels),
):
    conn = await ch.connect(websocket, channel, rewind=rewind, since=since)
    try:
        async for text in conn:
            await ch.publish(channel, {"text": text, "from": conn.conn_id},
                             sender_conn=conn.conn_id, exclude_sender=True)
    finally:
        await ch.disconnect(conn)
```

Run the full working example: `uv run uvicorn examples.example_fastapi_channels:app --port 8765`

## Stream rewind

Every published message is stored in a Redis Stream (`cy:chan:{channel}:stream`, trimmed to `stream_maxlen`). A new subscriber can request history on connect:

```
# Replay last 20 messages before live delivery
GET /ws/chat?rewind=20

# Lossless cursor-resume (use ts field from a previous message)
GET /ws/chat?since=1700000000-0
```

The `ts` field in every message envelope is the Redis Stream entry ID — use it as the `since` cursor on reconnect to receive exactly the messages you missed.

## Message envelope

```json
{
  "event":   "message",
  "channel": "chat",
  "sender":  "conn_abc123",
  "data":    { "text": "hello" },
  "ts":      "1700000000-0"
}
```

## Per-subscriber filters (server-side Lua)

Filter which messages each subscriber receives without touching Python on the hot path. The filter evaluation runs inside Redis via the `cy_channel_route` Lua Function.

```python
# Subscribe to only AAPL tickers
await ch.set_filter(conn_id, "tickers", {"symbol": "AAPL"})

# Switch to TSLA without reconnecting
await ch.set_filter(conn_id, "tickers", {"symbol": "TSLA"})

# Clear filter — receive everything
await ch.set_filter(conn_id, "tickers", None)
```

Or via REST (works cross-process — any Python process can update a subscriber's filter):

```
POST /channels/tickers/filter/{conn_id}
{"symbol": "AAPL"}
```

Filters are stored in the `cy:chan:{channel}:routes` Redis HASH. The Lua function reads this HASH and evaluates field-equality matches atomically. One `FCALL` replaces N Python-side filter checks.

## Presence

```python
members = await ch.get_members("chat")   # list of conn_ids, cross-server
count   = await ch.channel_count("chat")
```

Presence state lives in `cy:chan:{channel}:presence` (Redis HASH). All processes writing to the same Redis instance share the same view.

## Publish from anywhere

```python
# From a background task, REST handler, or another process
await ch.publish("alerts", {"level": "critical", "msg": "disk 90%"})
```

## History (without connecting)

```python
messages = await ch.history("chat", count=50)
messages = await ch.history("chat", since="1700000000-0")
```

Returns a list of message envelopes from the Redis Stream.

## Redis key layout

| Key | Type | Purpose |
|-----|------|---------|
| `cy:chan:{ch}:events` | pub/sub channel | Live message delivery |
| `cy:chan:{ch}:stream` | Stream | Persistent history (MAXLEN trimmed) |
| `cy:chan:{ch}:presence` | Hash | `conn_id → "local"` |
| `cy:chan:{ch}:routes` | Hash | `conn_id → json_filter` (empty = no filter) |
| `cy:chan:sub:{conn_id}:{ch}:cursor` | String | Last delivered stream entry ID |

## CyChannelManager API

```python
CyChannelManager(redis_client, stream_maxlen=1000, key_prefix="cy:chan")

# Lifecycle
await ch.start()   # load Lua function, start PSUBSCRIBE listener
await ch.stop()

# Connections
conn = await ch.connect(websocket, channel, conn_id=None,
                        rewind=None, since=None, filter_expr=None)
await ch.disconnect(conn)

# Channels
await ch.subscribe(conn_id, channel, filter_expr=None)
await ch.unsubscribe(conn_id, channel)
await ch.set_filter(conn_id, channel, filter_expr)   # dict or None

# Publishing
await ch.publish(channel, data, sender_conn=None, exclude_sender=False)
await ch.emit(conn_id, data)   # direct to one connection

# History and presence
messages = await ch.history(channel, count=None, since=None)
members  = await ch.get_members(channel)
count    = await ch.channel_count(channel)
```

## What to read next

- [Web layer](web.md) — web cache, JWT, sessions
- [Streams & integrations](streams.md) — Redis Streams, ClickHouse bridge
- [Scripting](scripting.md) — the Lua routing function in detail
- [Example](../examples/example_fastapi_channels.py) — complete working FastAPI app
