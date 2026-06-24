# Examples

← [README](../README.md)

All examples connect to Redis on `localhost:6379` by default. Run any of them with:

```bash
uv run python examples/<name>.py
```

## Core and data structures

| File | What it shows |
|------|--------------|
| `streaming_example.py` | Redis Streams and the data-structure async iterators (XADD/XREAD, list/pub-sub iteration) |
| `enhanced_cyredis_demo.py` | Broad feature tour (see note below) |
| `cluster_aware_demo.py` | Cluster command helpers and routing (see note below) |

## Web

| File | What it shows |
|------|--------------|
| `example_fastapi_channels.py` | `CyChannelManager` — WebSocket pub/sub, stream rewind, filters, presence (needs `fastapi`/`uvicorn`) |
| `web_app_example.py` | JWT tokens, sessions, 2FA, password reset |
| `web_cache_example.py` | `WebCache` set/get, the `cached_endpoint` decorator, namespace/pattern invalidation |
| `web_cache_simple_example.py` | Minimal web cache usage |

## Integrations

| File | What it shows |
|------|--------------|
| `example_clickhouse_redis.py` | ClickHouse bridge — live cache, stream dump, watch loop, channel broadcast |

> Note: `enhanced_cyredis_demo.py` and `cluster_aware_demo.py` currently import
> `CyRedisClientAsync`, which is not part of the public package, so they do not
> run as-is. The other scripts run against a local Redis on `localhost:6379`.

## FastAPI channels quick start

```bash
uv pip install fastapi uvicorn
uv run uvicorn examples.example_fastapi_channels:app --reload --port 8765
```

Then in another terminal:

```python
import asyncio, websockets, json
async def run():
    async with websockets.connect("ws://localhost:8765/ws/demo") as ws:
        await ws.send("hello")
        print(json.loads(await asyncio.wait_for(ws.recv(), 3)))
asyncio.run(run())
```

## ClickHouse bridge quick start

```bash
# Requires ClickHouse at localhost:8123 and Redis at localhost:6379
uv run python examples/example_clickhouse_redis.py

# With live WebSocket server (mode 4)
WITH_WS=1 uv run python examples/example_clickhouse_redis.py
```
