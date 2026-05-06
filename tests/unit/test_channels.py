"""
Unit tests for CyChannelManager and CyChannelConnection.

All tests use a MockWebSocket and a MockRedisClient — no running Redis or
FastAPI server required.  The mock client records every execute_command /
publish call so assertions can inspect Redis interactions without network I/O.
"""
import asyncio
import json
import uuid
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

class MockWebSocket:
    """ASGI WebSocket stub backed by asyncio queues."""

    def __init__(self):
        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._recv_queue: asyncio.Queue = asyncio.Queue()
        self.accepted = False
        self.closed = False
        self.close_code = None

    async def accept(self):
        self.accepted = True

    async def send_text(self, text: str):
        await self._send_queue.put(text)

    async def receive_text(self) -> str:
        return await asyncio.wait_for(self._recv_queue.get(), timeout=2.0)

    async def close(self, code: int = 1000):
        self.closed = True
        self.close_code = code

    async def inject(self, text: str):
        """Simulate the client sending *text*."""
        await self._recv_queue.put(text)

    async def drain(self, timeout: float = 0.1) -> list:
        """Collect all buffered outbound messages."""
        msgs = []
        while True:
            try:
                raw = await asyncio.wait_for(self._send_queue.get(), timeout=timeout)
                msgs.append(json.loads(raw))
            except asyncio.TimeoutError:
                break
        return msgs


class MockRedisClient:
    """
    Minimal CyRedisClient mock.

    Tracks execute_command and publish calls.  Provides enough of the
    connection pool interface to satisfy CyChannelManager.
    """

    def __init__(self):
        self._store: dict = {}         # simple key-value store
        self._hashes: dict = {}        # HASH key -> {field: value}
        self._streams: dict = {}       # stream key -> [{id, payload}]
        self._pubsub_calls: list = []  # (channel, message) tuples
        self._commands: list = []      # all execute_command calls
        self._stream_counter: int = 0

    # --- Connection pool stubs (used by RedisPSubIterator) ---
    @property
    def pool(self):
        m = MagicMock()
        m.get_host.return_value = 'localhost'
        m.get_port.return_value = 6379
        return m

    def publish(self, channel: str, message: str) -> int:
        self._pubsub_calls.append((channel, message))
        return 0

    def execute_command(self, args: list):
        self._commands.append(args)
        cmd = args[0].upper() if args else ''

        if cmd == 'XADD':
            # XADD stream_key MAXLEN ~ N * field value
            stream_key = args[1]
            self._stream_counter += 1
            entry_id = f"1000000{self._stream_counter:06d}-0"
            # Find payload in args
            try:
                p_idx = args.index('payload')
                payload = args[p_idx + 1]
            except (ValueError, IndexError):
                payload = ''
            self._streams.setdefault(stream_key, []).append(
                (entry_id, payload)
            )
            return entry_id.encode()

        if cmd == 'XREVRANGE':
            stream_key = args[1]
            count = int(args[5]) if len(args) > 5 else 50
            entries = self._streams.get(stream_key, [])[-count:]
            # Return newest-first
            result = []
            for eid, payload in reversed(entries):
                result.append([eid.encode(), [b'payload', payload.encode()
                               if isinstance(payload, str) else payload]])
            return result

        if cmd == 'XRANGE':
            stream_key = args[1]
            since_raw = args[2]
            entries = self._streams.get(stream_key, [])
            if since_raw.startswith('('):
                since = since_raw[1:]
                entries = [(eid, p) for eid, p in entries if eid > since]
            result = []
            for eid, payload in entries:
                result.append([eid.encode(), [b'payload', payload.encode()
                               if isinstance(payload, str) else payload]])
            return result

        if cmd == 'HSET':
            key, field, value = args[1], args[2], args[3]
            self._hashes.setdefault(key, {})[field] = value
            return 1

        if cmd == 'HDEL':
            key, field = args[1], args[2]
            self._hashes.get(key, {}).pop(field, None)
            return 1

        if cmd == 'HKEYS':
            key = args[1]
            return [k.encode() if isinstance(k, str) else k
                    for k in self._hashes.get(key, {}).keys()]

        if cmd == 'HLEN':
            return len(self._hashes.get(args[1], {}))

        if cmd == 'HGETALL':
            items = []
            for k, v in self._hashes.get(args[1], {}).items():
                items.append(k.encode() if isinstance(k, str) else k)
                items.append(v.encode() if isinstance(v, str) else v)
            return items

        if cmd == 'SET':
            self._store[args[1]] = args[2]
            return b'OK'

        if cmd == 'GET':
            val = self._store.get(args[1])
            return val.encode() if isinstance(val, str) else val

        if cmd in ('FUNCTION', 'FCALL'):
            # FUNCTION LOAD -> OK; FCALL cy_channel_route -> [] (all pass in tests)
            if cmd == 'FUNCTION':
                return b'OK'
            # FCALL cy_channel_route 1 routes_key message -> all keys in routes hash
            if len(args) >= 4:
                routes_key = args[3]
                return [k.encode() if isinstance(k, str) else k
                        for k in self._hashes.get(routes_key, {}).keys()]
            return []

        return None


def make_manager(maxlen=100):
    redis = MockRedisClient()
    # Patch out RedisPSubIterator so tests don't need a real connection
    from cy_redis.web.channels import CyChannelManager
    mgr = CyChannelManager(redis, stream_maxlen=maxlen)
    return mgr, redis


async def start_manager_no_psub(mgr):
    """Start manager without spinning up the real PSUBSCRIBE task."""
    mgr._running = True
    mgr._routing_loaded = True   # skip Lua load


# ---------------------------------------------------------------------------
# CyChannelConnection tests
# ---------------------------------------------------------------------------

class TestCyChannelConnection:

    @pytest.mark.asyncio
    async def test_send_text(self):
        from cy_redis.web.channels import CyChannelConnection
        ws = MockWebSocket()
        conn = CyChannelConnection(ws, "c1")
        await conn.send("hello")
        assert (await ws._send_queue.get()) == "hello"

    @pytest.mark.asyncio
    async def test_send_json(self):
        from cy_redis.web.channels import CyChannelConnection
        ws = MockWebSocket()
        conn = CyChannelConnection(ws, "c2")
        await conn.send_json({"k": "v"})
        raw = await ws._send_queue.get()
        assert json.loads(raw) == {"k": "v"}

    @pytest.mark.asyncio
    async def test_async_iter_from_client(self):
        from cy_redis.web.channels import CyChannelConnection
        ws = MockWebSocket()
        conn = CyChannelConnection(ws, "c3")
        await ws.inject("msg1")
        received = await conn.__anext__()
        assert received == "msg1"

    @pytest.mark.asyncio
    async def test_closed_raises_stop(self):
        from cy_redis.web.channels import CyChannelConnection
        ws = MockWebSocket()
        conn = CyChannelConnection(ws, "c4")
        conn._closed = True
        with pytest.raises(StopAsyncIteration):
            await conn.__anext__()

    def test_get_channels_empty(self):
        from cy_redis.web.channels import CyChannelConnection
        ws = MockWebSocket()
        conn = CyChannelConnection(ws, "c5")
        assert conn.get_channels() == set()

    def test_repr(self):
        from cy_redis.web.channels import CyChannelConnection
        ws = MockWebSocket()
        conn = CyChannelConnection(ws, "my-id")
        assert "my-id" in repr(conn)


# ---------------------------------------------------------------------------
# Key format tests
# ---------------------------------------------------------------------------

class TestKeyFormat:

    def test_pubsub_key(self):
        mgr, _ = make_manager()
        assert mgr._pubsub_key("stocks") == "cy:chan:stocks:events"

    def test_stream_key(self):
        mgr, _ = make_manager()
        assert mgr._stream_key("stocks") == "cy:chan:stocks:stream"

    def test_presence_key(self):
        mgr, _ = make_manager()
        assert mgr._presence_key("stocks") == "cy:chan:stocks:presence"

    def test_routes_key(self):
        mgr, _ = make_manager()
        assert mgr._routes_key("stocks") == "cy:chan:stocks:routes"

    def test_cursor_key(self):
        mgr, _ = make_manager()
        assert mgr._sub_cursor_key("conn1", "stocks") == "cy:chan:sub:conn1:stocks:cursor"

    def test_custom_prefix(self):
        from cy_redis.web.channels import CyChannelManager
        redis = MockRedisClient()
        mgr = CyChannelManager(redis, key_prefix="myapp")
        assert mgr._pubsub_key("chat") == "myapp:chat:events"


# ---------------------------------------------------------------------------
# Connect / subscribe / unsubscribe
# ---------------------------------------------------------------------------

class TestConnectSubscribe:

    @pytest.mark.asyncio
    async def test_connect_accepts_websocket(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "chat")
        assert ws.accepted
        assert conn.conn_id in mgr._connections

    @pytest.mark.asyncio
    async def test_connect_registers_presence(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "chat")
        presence_key = mgr._presence_key("chat")
        assert conn.conn_id in redis._hashes.get(presence_key, {})

    @pytest.mark.asyncio
    async def test_connect_registers_route(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "chat")
        routes_key = mgr._routes_key("chat")
        assert conn.conn_id in redis._hashes.get(routes_key, {})

    @pytest.mark.asyncio
    async def test_connect_with_filter(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "tickers", filter_expr={"symbol": "AAPL"})
        routes_key = mgr._routes_key("tickers")
        stored_filter = redis._hashes.get(routes_key, {}).get(conn.conn_id, '')
        assert json.loads(stored_filter) == {"symbol": "AAPL"}

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "chat")
        conn_id = conn.conn_id
        await mgr.disconnect(conn)
        assert conn_id not in mgr._connections
        presence_key = mgr._presence_key("chat")
        assert conn_id not in redis._hashes.get(presence_key, {})

    @pytest.mark.asyncio
    async def test_get_subscriptions(self):
        mgr, _ = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "channelA")
        await mgr.subscribe(conn.conn_id, "channelB")
        subs = await mgr.get_subscriptions(conn.conn_id)
        assert set(subs) == {"channelA", "channelB"}


# ---------------------------------------------------------------------------
# Publish / deliver
# ---------------------------------------------------------------------------

class TestPublishDeliver:

    @pytest.mark.asyncio
    async def test_publish_writes_to_stream(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        await mgr.publish("chat", {"text": "hello"})
        stream_key = mgr._stream_key("chat")
        assert len(redis._streams.get(stream_key, [])) == 1

    @pytest.mark.asyncio
    async def test_publish_sends_to_pubsub(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        await mgr.publish("chat", {"text": "hi"})
        pubsub_key = mgr._pubsub_key("chat")
        assert any(ch == pubsub_key for ch, _ in redis._pubsub_calls)

    @pytest.mark.asyncio
    async def test_deliver_local_reaches_subscriber(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "chat")

        envelope = json.dumps({
            "event": "message", "channel": "chat",
            "sender": "other", "exclude_sender": False,
            "data": {"text": "hello"}, "ts": "100-0",
        })
        await mgr._deliver_local("chat", envelope)

        msgs = await ws.drain()
        assert len(msgs) == 1
        assert msgs[0]["data"]["text"] == "hello"

    @pytest.mark.asyncio
    async def test_exclude_sender_skips_originator(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws1 = MockWebSocket()
        ws2 = MockWebSocket()
        conn1 = await mgr.connect(ws1, "chat")
        conn2 = await mgr.connect(ws2, "chat")

        envelope = json.dumps({
            "event": "message", "channel": "chat",
            "sender": conn1.conn_id, "exclude_sender": True,
            "data": {"text": "from conn1"}, "ts": "200-0",
        })
        await mgr._deliver_local("chat", envelope)

        msgs1 = await ws1.drain()
        msgs2 = await ws2.drain()
        assert len(msgs1) == 0          # sender excluded
        assert len(msgs2) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_delivery(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "chat")
        await mgr.unsubscribe(conn.conn_id, "chat")

        envelope = json.dumps({
            "event": "message", "channel": "chat",
            "sender": "x", "exclude_sender": False,
            "data": {"text": "nope"}, "ts": "300-0",
        })
        await mgr._deliver_local("chat", envelope)
        msgs = await ws.drain()
        assert len(msgs) == 0

    @pytest.mark.asyncio
    async def test_emit_direct(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "chat")
        await mgr.emit(conn.conn_id, {"secret": True})
        msgs = await ws.drain()
        assert len(msgs) == 1
        assert msgs[0]["secret"] is True


# ---------------------------------------------------------------------------
# Stream history / rewind
# ---------------------------------------------------------------------------

class TestHistory:

    @pytest.mark.asyncio
    async def test_history_returns_empty_for_new_channel(self):
        mgr, _ = make_manager()
        result = await mgr.history("empty_channel")
        assert result == []

    @pytest.mark.asyncio
    async def test_history_last_n(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        # Publish 5 messages
        for i in range(5):
            await mgr.publish("news", {"i": i})

        result = await mgr.history("news", count=3)
        assert len(result) == 3
        # Should be chronological (oldest first)
        assert result[0]["data"]["i"] < result[-1]["data"]["i"]

    @pytest.mark.asyncio
    async def test_history_since(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        for i in range(4):
            await mgr.publish("news", {"i": i})

        # Get all history first
        all_msgs = await mgr.history("news")
        assert len(all_msgs) == 4

        # Request history since the 2nd message's ts
        cursor = all_msgs[1]["ts"]
        after = await mgr.history("news", since=cursor)
        # Should return messages 3 and 4 (indices 2 and 3)
        assert len(after) == 2
        assert after[0]["data"]["i"] == 2
        assert after[1]["data"]["i"] == 3

    @pytest.mark.asyncio
    async def test_rewind_on_connect_delivers_history(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        # Pre-populate stream via publish before connecting
        for i in range(3):
            await mgr.publish("live", {"n": i})

        ws = MockWebSocket()
        conn = await mgr.connect(ws, "live", rewind=3)
        msgs = await ws.drain()
        assert len(msgs) == 3
        assert msgs[0]["data"]["n"] == 0
        assert msgs[2]["data"]["n"] == 2


# ---------------------------------------------------------------------------
# Presence
# ---------------------------------------------------------------------------

class TestPresence:

    @pytest.mark.asyncio
    async def test_get_members(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws1, ws2 = MockWebSocket(), MockWebSocket()
        c1 = await mgr.connect(ws1, "room")
        c2 = await mgr.connect(ws2, "room")
        members = await mgr.get_members("room")
        assert c1.conn_id in members
        assert c2.conn_id in members

    @pytest.mark.asyncio
    async def test_channel_count(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws1, ws2 = MockWebSocket(), MockWebSocket()
        await mgr.connect(ws1, "room")
        await mgr.connect(ws2, "room")
        assert await mgr.channel_count("room") == 2

    @pytest.mark.asyncio
    async def test_channel_count_decrements_on_disconnect(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "room")
        assert await mgr.channel_count("room") == 1
        await mgr.disconnect(conn)
        assert await mgr.channel_count("room") == 0


# ---------------------------------------------------------------------------
# Set-filter (live subscription swizzle)
# ---------------------------------------------------------------------------

class TestSetFilter:

    @pytest.mark.asyncio
    async def test_set_filter_updates_redis(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "tickers")
        await mgr.set_filter(conn.conn_id, "tickers", {"symbol": "TSLA"})
        routes_key = mgr._routes_key("tickers")
        stored = redis._hashes.get(routes_key, {}).get(conn.conn_id, '')
        assert json.loads(stored) == {"symbol": "TSLA"}

    @pytest.mark.asyncio
    async def test_set_filter_clear(self):
        mgr, redis = make_manager()
        await start_manager_no_psub(mgr)
        ws = MockWebSocket()
        conn = await mgr.connect(ws, "tickers", filter_expr={"symbol": "AAPL"})
        # Clear the filter
        await mgr.set_filter(conn.conn_id, "tickers", None)
        routes_key = mgr._routes_key("tickers")
        stored = redis._hashes.get(routes_key, {}).get(conn.conn_id, '')
        assert stored == ''


# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------

class TestCursor:

    @pytest.mark.asyncio
    async def test_save_and_load_cursor(self):
        mgr, redis = make_manager()
        await mgr.save_cursor("conn1", "news", "1234567890-0")
        cursor = await mgr._load_cursor("conn1", "news")
        assert cursor == "1234567890-0"

    @pytest.mark.asyncio
    async def test_load_cursor_returns_none_if_missing(self):
        mgr, _ = make_manager()
        cursor = await mgr._load_cursor("nonexistent", "news")
        assert cursor is None
