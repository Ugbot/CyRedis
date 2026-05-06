# Examples

← [README](../README.md)

All examples connect to Redis on `localhost:6379` by default. Run any of them with:

```bash
uv run python examples/<name>.py
```

## Core

| File | What it shows |
|------|--------------|
| `example_usage.py` | Basic get/set, lists, hashes, async vs sync |
| `example_protocol_support.py` | RESP2/RESP3, pipelining, connection pooling |
| `example_production_redis.py` | Connection retries, health checks, production patterns |
| `production_redis.py` | Alternate production config example |
| `migration_example.py` | Migrating from redis-py |

## Scripting

| File | What it shows |
|------|--------------|
| `example_lua_scripts.py` | EVAL, EVALSHA, pre-built lua_scripts/ |
| `example_redis_functions.py` | FUNCTION LOAD, FCALL, Redis Functions libraries |
| `example_atomic_script_deployment.py` | ScriptManager, hot-reload, all-or-nothing deployment |

## Data structures and distributed

| File | What it shows |
|------|--------------|
| `example_shared_dict.py` | SharedDict, ConcurrentSharedDict across processes |
| `streaming_example.py` | Redis Streams, XADD/XREAD, async stream iteration |

## Web

| File | What it shows |
|------|--------------|
| `example_fastapi_channels.py` | CyChannelManager — WebSocket pub/sub, stream rewind, filters, presence |
| `web_app_example.py` | JWT tokens, sessions, 2FA, password reset |
| `web_cache_example.py` | WebCache decorator, tags, invalidation |
| `web_cache_simple_example.py` | Minimal web cache usage |

## Integrations

| File | What it shows |
|------|--------------|
| `example_clickhouse_redis.py` | ClickHouse bridge — live cache, stream dump, watch loop, channel broadcast |

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
