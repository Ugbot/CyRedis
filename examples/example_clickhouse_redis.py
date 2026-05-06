"""
CyRedis — ClickHouse→Redis bridge example (stock tickers).

Demonstrates four integration patterns:

  Mode 1 — Live cache
    ClickHouse materializes the latest bid/ask/price for each symbol into
    Redis key-value pairs via a Redis-engine table + MaterializedView.
    After setup, ClickHouse writes directly to Redis with no Python loop.

  Mode 2 — Stream dump
    One-shot: dump the last hour of raw tick data into a Redis Stream so
    downstream consumers (WebSocket subscribers, analytics) can replay it.

  Mode 3 — Watch stream
    Async polling loop: as new ticks arrive in ClickHouse the bridge
    XADDs them to a Redis Stream, advancing a watermark so no row is
    processed twice.

  Mode 4 — Channel broadcast
    Consumes the Redis Stream from mode 3 and publishes each entry to a
    CyChannelManager channel so connected WebSocket clients see live events.
    Requires fastapi + uvicorn: uv pip install fastapi uvicorn websockets

Prerequisites:
  ClickHouse running at localhost:8123 (uv run clickhouse server / Docker)
  Redis running at localhost:6379

Run:
  uv run python examples/example_clickhouse_redis.py

  # With channel broadcast WebSocket server:
  WITH_WS=1 uv run python examples/example_clickhouse_redis.py
"""

import asyncio
import os
import random
import sys
import time
from datetime import datetime, timedelta

from cy_redis import CyRedisClient
from cy_redis.integrations.clickhouse import CyClickHouseBridge, CyClickHouseClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
WITH_WS = os.getenv("WITH_WS", "0") == "1"

SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN", "GOOGL", "META", "NFLX"]
STREAM_KEY = "cy:ticks:stream"
CHANNEL_NAME = "tickers"


# ---------------------------------------------------------------------------
# Seed ClickHouse with synthetic tick data
# ---------------------------------------------------------------------------

async def setup_clickhouse(ch: CyClickHouseClient) -> None:
    """Create ticks table and insert synthetic data if it doesn't exist."""
    print("[setup] Creating ticks table...")
    await ch.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            ts       DateTime DEFAULT now(),
            symbol   LowCardinality(String),
            price    Float64,
            bid      Float64,
            ask      Float64,
            volume   UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (symbol, ts)
        TTL ts + INTERVAL 7 DAY
    """)

    row_count_rows = await ch.query("SELECT count() AS n FROM ticks")
    existing = int(row_count_rows[0]["n"]) if row_count_rows else 0
    if existing > 0:
        print(f"[setup] ticks table has {existing} rows — skipping seed")
        return

    print("[setup] Seeding 2 000 tick rows...")
    now = datetime.utcnow()
    rows = []
    for i in range(2000):
        symbol = random.choice(SYMBOLS)
        price = round(100 + random.uniform(-10, 50), 4)
        spread = round(random.uniform(0.01, 0.10), 4)
        ts = now - timedelta(seconds=random.randint(0, 3600))
        rows.append(
            f"('{ts.strftime('%Y-%m-%d %H:%M:%S')}', '{symbol}', "
            f"{price}, {price - spread}, {price + spread}, {random.randint(1, 1000)})"
        )

    values_sql = "INSERT INTO ticks (ts, symbol, price, bid, ask, volume) VALUES " + ",".join(rows)
    await ch.execute(values_sql)
    print("[setup] Seed complete.")


# ---------------------------------------------------------------------------
# Mode 1 — Live cache
# ---------------------------------------------------------------------------

async def demo_live_cache(bridge: CyClickHouseBridge) -> None:
    print("\n" + "=" * 60)
    print("MODE 1 — Live cache via Redis engine + MaterializedView")
    print("=" * 60)

    source_query = """
        SELECT
            symbol,
            round(argMax(price, ts), 4)  AS last_price,
            round(argMax(bid,   ts), 4)  AS last_bid,
            round(argMax(ask,   ts), 4)  AS last_ask,
            max(ts)                      AS updated_at
        FROM ticks
        GROUP BY symbol
    """

    result = await bridge.create_live_cache(
        name="ticker_latest",
        source_query=source_query,
        key_column="symbol",
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        redis_key_prefix="tick:",
        refresh_interval_sec=0,  # incremental MV
    )

    print(f"  Created table : {result['table']}")
    print(f"  Created view  : {result['view']}")
    print("\n  Generated DDL:")
    for ddl in result["ddl"]:
        for line in ddl.strip().splitlines():
            print(f"    {line}")
        print()

    print("  ClickHouse will now push changes to Redis automatically.")
    print("  Redis key pattern: tick:<symbol>  (msgpack blob value)")


# ---------------------------------------------------------------------------
# Mode 2 — Stream dump
# ---------------------------------------------------------------------------

async def demo_stream_dump(bridge: CyClickHouseBridge, redis: CyRedisClient) -> None:
    print("\n" + "=" * 60)
    print("MODE 2 — One-shot stream dump into Redis Stream")
    print("=" * 60)

    cutoff = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        SELECT ts, symbol, price, bid, ask, volume
        FROM ticks
        WHERE ts >= '{cutoff}'
        ORDER BY ts
    """

    print(f"  Dumping ticks from last hour to {STREAM_KEY!r}...")
    t0 = time.monotonic()
    count = await bridge.dump_to_stream(query, STREAM_KEY, batch_size=200, maxlen=50_000)
    elapsed = time.monotonic() - t0
    print(f"  Written {count} rows in {elapsed:.2f}s")

    stream_len = await asyncio.get_event_loop().run_in_executor(
        None, redis.xlen, STREAM_KEY
    )
    print(f"  Redis stream length: {stream_len}")

    # Show a sample of what was written
    sample = await redis.xread_async({STREAM_KEY: "0-0"}, count=3, block=0)
    if sample:
        print("  Sample entries:")
        for _stream, entry_id, fields in sample:
            print(f"    [{entry_id}] {dict(fields)}")


# ---------------------------------------------------------------------------
# Mode 3 — Watch stream (runs for a short burst then cancels)
# ---------------------------------------------------------------------------

async def demo_watch_stream(bridge: CyClickHouseBridge, ch: CyClickHouseClient) -> None:
    print("\n" + "=" * 60)
    print("MODE 3 — Incremental watch → Redis Stream (15-second demo)")
    print("=" * 60)

    watch_stream_key = "cy:ticks:live"
    query = """
        SELECT ts, symbol, price, bid, ask, volume
        FROM ticks
        WHERE ts > '{watermark}'
        ORDER BY ts
        LIMIT 500
    """

    initial_wm = (datetime.utcnow() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  Watching for new ticks since {initial_wm!r}...")
    print(f"  Writing to Redis Stream: {watch_stream_key!r}")

    poll_count = 0
    total_rows = 0

    # Insert fresh rows every few seconds to simulate live traffic
    async def insert_live_ticks():
        for _ in range(5):
            await asyncio.sleep(2)
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            symbol = random.choice(SYMBOLS)
            price = round(100 + random.uniform(-10, 50), 4)
            spread = round(random.uniform(0.01, 0.05), 4)
            await ch.execute(
                f"INSERT INTO ticks (ts, symbol, price, bid, ask, volume) VALUES "
                f"('{ts}', '{symbol}', {price}, {price-spread}, {price+spread}, "
                f"{random.randint(1, 500)})"
            )

    inserter = asyncio.create_task(insert_live_ticks())

    async def run_watcher():
        nonlocal poll_count, total_rows
        async for batch_count in bridge.watch_as_stream(
            query=query,
            watermark_column="ts",
            stream_key=watch_stream_key,
            poll_interval_sec=2.0,
            maxlen=10_000,
            initial_watermark=initial_wm,
        ):
            poll_count += 1
            total_rows += batch_count
            print(f"  Poll {poll_count:2d}: +{batch_count} rows  (total so far: {total_rows})")
            if poll_count >= 7:
                break

    await asyncio.gather(run_watcher(), inserter)
    print(f"  Watcher finished — {total_rows} rows added across {poll_count} polls.")


# ---------------------------------------------------------------------------
# Mode 4 — Channel broadcast (WebSocket server, optional)
# ---------------------------------------------------------------------------

async def demo_channel_broadcast(
    bridge: CyClickHouseBridge,
    redis: CyRedisClient,
) -> None:
    print("\n" + "=" * 60)
    print("MODE 4 — Stream → CyChannelManager channel broadcast")
    print("=" * 60)

    try:
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        import uvicorn

        from cy_redis.web import CyChannelManager, create_redis_lifespan, get_channels
    except ImportError:
        print(
            "  fastapi/uvicorn not installed — skipping.\n"
            "  Install with: uv pip install fastapi uvicorn websockets"
        )
        return

    channel_manager = CyChannelManager(redis, stream_maxlen=1000)
    app = FastAPI(
        title="CyRedis Tickers Demo",
        lifespan=create_redis_lifespan(redis, channel_manager),
    )

    @app.websocket("/ws/tickers")
    async def ws_tickers(websocket: WebSocket):
        from cy_redis.web import get_channels as _gc
        ch = channel_manager
        conn = await ch.connect(websocket, CHANNEL_NAME, rewind=10)
        try:
            async for _ in conn:
                pass  # read-only channel — ignore client messages
        except WebSocketDisconnect:
            pass
        finally:
            await ch.disconnect(conn)

    # Start the stream→channel bridge in background
    async def broadcast_task():
        await asyncio.sleep(1)  # wait for channel_manager to start
        await bridge.stream_to_channel(
            stream_key="cy:ticks:live",
            channel=CHANNEL_NAME,
            channel_manager=channel_manager,
            batch_size=50,
            poll_interval_sec=1.0,
        )

    print(f"  WebSocket server at ws://localhost:8766/ws/tickers")
    print(f"  Connect with: python -c \"")
    print(f"    import asyncio, websockets, json")
    print(f"    async def r():")
    print(f"        async with websockets.connect('ws://localhost:8766/ws/tickers') as ws:")
    print(f"            for _ in range(10): print(json.loads(await ws.recv()))")
    print(f"    asyncio.run(r())\"")

    broadcast = asyncio.create_task(broadcast_task())

    config = uvicorn.Config(app, host="0.0.0.0", port=8766, log_level="warning")
    server = uvicorn.Server(config)
    try:
        await asyncio.wait_for(server.serve(), timeout=20)
    except asyncio.TimeoutError:
        pass
    finally:
        broadcast.cancel()
        try:
            await broadcast
        except asyncio.CancelledError:
            pass

    print("  Demo server stopped.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    ch = CyClickHouseClient(host=CH_HOST, port=CH_PORT)
    redis = CyRedisClient(host=REDIS_HOST, port=REDIS_PORT)
    bridge = CyClickHouseBridge(ch, redis)

    # Connectivity checks
    if not await ch.ping():
        print(
            f"ERROR: ClickHouse not reachable at {CH_HOST}:{CH_PORT}\n"
            "Start ClickHouse and retry."
        )
        sys.exit(1)

    version = await ch.server_version()
    print(f"Connected to ClickHouse {version}")
    print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

    await setup_clickhouse(ch)

    await demo_live_cache(bridge)
    await demo_stream_dump(bridge, redis)
    await demo_watch_stream(bridge, ch)

    if WITH_WS:
        await demo_channel_broadcast(bridge, redis)
    else:
        print(
            "\nMode 4 (channel broadcast) skipped — set WITH_WS=1 to run the WebSocket server."
        )

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
