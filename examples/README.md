# Examples

← [README](../README.md)

All examples connect to Redis on `localhost:6379` by default. Run any of them with:

```bash
uv run python examples/<name>.py
```

Every example here talks to a real server through the public API; there are
no mock-backed demos. `make test-examples` (also run by CI's packaging job)
imports each file against the installed package, so an example that drifts
from the shipped API fails the build.

## Core (supported `cy-redis` wheel)

| File | What it shows |
|------|--------------|
| `enhanced_cyredis_demo.py` | Broad feature tour: sync + async clients, pipelines, streams, capability detection |
| `cluster_aware_demo.py` | `CyRedisCluster`: topology, slot routing, cross-slot `mget`/`mset`, cluster pipeline (needs a running cluster; `REDIS_CLUSTER_NODES`) |

## Web (experimental)

These import `cyredis_experimental.web`, which is not part of the `cy-redis`
wheel — install it from [`experimental/`](../experimental/README.md) first
(`uv pip install --no-build-isolation -e "./experimental[web]"`).

| File | What it shows |
|------|--------------|
| `example_fastapi_channels.py` | `CyChannelManager` — WebSocket pub/sub, stream rewind, filters, presence (needs `fastapi`/`uvicorn`) |
| `web_cache_example.py` | `WebCache` set/get, the `cached_endpoint` decorator, namespace/pattern invalidation |

## Integrations (experimental)

The ClickHouse bridge is `cyredis_experimental.extras.clickhouse`, also outside
the wheel.

| File | What it shows |
|------|--------------|
| `example_clickhouse_redis.py` | ClickHouse bridge — live cache, stream dump, watch loop, channel broadcast |

## Game engine (experimental)

| File | What it shows |
|------|--------------|
| `game_engine_example.py` | `cyredis_experimental.game` — zones, entities, intents, ticks; needs Redis 7+ (`FUNCTION`) and `experimental[game]` installed |

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
