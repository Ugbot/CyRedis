"""
CyRedis — FastAPI WebSocket channels example.

Features demonstrated:
- Distributed pub/sub channels backed by Redis
- Stream rewind: ?rewind=N delivers last N messages on connect
- Cursor resume: ?since=<stream_id> for lossless reconnect
- Live filter swizzle: POST /channels/{ch}/filter changes what a subscriber sees
- Server-side Lua routing (field-match filtering stays inside Redis)
- Presence: GET /channels/{ch}/members shows all connected clients (cross-server)
- History: GET /channels/{ch}/history serves archived messages from Redis Stream

Run:
    uv pip install fastapi uvicorn
    uv run uvicorn examples.example_fastapi_channels:app --reload --port 8765

Quick smoke-test (separate terminal):
    python -c "
    import asyncio, websockets, json
    async def run():
        async with websockets.connect('ws://localhost:8765/ws/demo') as ws:
            await ws.send('hello')
            print(json.loads(await asyncio.wait_for(ws.recv(), 3)))
    asyncio.run(run())
    "
"""

from fastapi import Depends, FastAPI, Query, WebSocket, WebSocketDisconnect

from cy_redis import CyRedisClient
from cy_redis.web import CyChannelManager, create_redis_lifespan, get_channels

# ---------------------------------------------------------------------------
# App bootstrap
# ---------------------------------------------------------------------------
redis_client = CyRedisClient(host="localhost", port=6379)
channel_manager = CyChannelManager(redis_client, stream_maxlen=1000)

app = FastAPI(
    title="CyRedis Channels Demo",
    lifespan=create_redis_lifespan(redis_client, channel_manager),
)


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------


@app.websocket("/ws/{channel}")
async def websocket_endpoint(
    websocket: WebSocket,
    channel: str,
    rewind: int = Query(None, description="Replay last N messages on connect"),
    since: str = Query(None, description="Replay messages after this stream ID"),
    ch: CyChannelManager = Depends(get_channels),
):
    """
    Connect to *channel*.

    Query parameters
    ----------------
    rewind:
        Replay the last *N* messages from the channel's history stream before
        switching to live delivery.
    since:
        Resume from a specific stream cursor (use the ``ts`` field from a
        previous message for a lossless reconnect).

    Message format (server → client)::

        {
          "event":   "message",
          "channel": "demo",
          "sender":  "<conn_id>",
          "data":    { ... },
          "ts":      "1700000000-0"   ← Redis Stream entry ID / cursor
        }
    """
    conn = await ch.connect(websocket, channel, rewind=rewind, since=since)
    try:
        async for text in conn:
            # Echo client text to all other subscribers on the channel
            await ch.publish(
                channel,
                {"text": text, "from": conn.conn_id},
                sender_conn=conn.conn_id,
                exclude_sender=True,
            )
    except WebSocketDisconnect:
        pass
    finally:
        await ch.disconnect(conn)


# ---------------------------------------------------------------------------
# REST management endpoints
# ---------------------------------------------------------------------------


@app.post("/channels/{channel}/publish")
async def publish_message(
    channel: str,
    body: dict,
    ch: CyChannelManager = Depends(get_channels),
):
    """Publish *body* to *channel* from the server side (not via WebSocket)."""
    await ch.publish(channel, body)
    return {
        "ok": True,
        "channel": channel,
        "members": await ch.channel_count(channel),
    }


@app.get("/channels/{channel}/members")
async def list_members(
    channel: str,
    ch: CyChannelManager = Depends(get_channels),
):
    """Return all conn_ids currently subscribed to *channel* (cross-server)."""
    return {
        "channel": channel,
        "members": await ch.get_members(channel),
        "count": await ch.channel_count(channel),
    }


@app.get("/channels/{channel}/history")
async def get_history(
    channel: str,
    count: int = Query(50, ge=1, le=1000),
    since: str = Query(None, description="Return messages after this stream ID"),
    ch: CyChannelManager = Depends(get_channels),
):
    """
    Fetch archived messages from the channel's Redis Stream.

    Useful for building "load earlier messages" UI or for server-side replay.
    """
    messages = await ch.history(channel, count=count, since=since)
    return {"channel": channel, "messages": messages}


@app.post("/channels/{channel}/subscribe/{conn_id}")
async def update_subscription(
    channel: str,
    conn_id: str,
    body: dict,
    ch: CyChannelManager = Depends(get_channels),
):
    """
    Subscribe *conn_id* to *channel* (or update their filter) without going
    through the WebSocket.  Call this from any Python process — the change is
    written directly to Redis and is effective immediately.

    This is how you implement a "big stream → per-subscriber slice" pattern:

    1. All stock-ticker data arrives on channel ``tickers``.
    2. A trading bot connects and calls this endpoint with
       ``{"symbol": "AAPL"}`` to receive only AAPL messages.
    3. Later it calls again with ``{"symbol": "TSLA"}`` to switch symbols
       without reconnecting.

    The routing decision runs inside Redis via the ``cy_channel_route``
    Lua Function — no extra Python round-trip per message.
    """
    filter_expr = body.get("filter") or None
    await ch.set_filter(conn_id, channel, filter_expr)
    return {
        "ok": True,
        "conn_id": conn_id,
        "channel": channel,
        "filter": filter_expr,
    }


@app.post("/channels/{channel}/filter/{conn_id}")
async def update_filter(
    channel: str,
    conn_id: str,
    body: dict,
    ch: CyChannelManager = Depends(get_channels),
):
    """
    Update the server-side delivery filter for *conn_id* on *channel*.

    The filter is a flat dict of ``field: value`` pairs evaluated against
    the ``data`` payload of each published message.  Only messages whose
    data matches **all** pairs are forwarded to the subscriber.

    Pass ``{}`` to clear the filter and receive all messages.

    Example — subscribe conn_id to AAPL tickers only::

        POST /channels/tickers/filter/abc123
        {"symbol": "AAPL"}
    """
    await ch.set_filter(conn_id, channel, body or None)
    return {"ok": True, "conn_id": conn_id, "channel": channel, "filter": body}
