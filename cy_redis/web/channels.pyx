# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False

"""
Distributed WebSocket channels with Redis-backed routing and stream rewind.

Architecture:
- Each channel is backed by a Redis pub/sub channel (live) and a Redis Stream (history)
- One PSUBSCRIBE connection per CyChannelManager covers all channels
- Subscription routing table stored in Redis HASH — can be updated live from any process
- Server-side field-match filtering via Lua Function (cy_channel_route)
- Stream cursors per (connection, channel) allow no-missed-message reconnects

Redis key layout (prefix = "cy:chan"):
  {prefix}:{channel}:events      — pub/sub channel for live delivery
  {prefix}:{channel}:stream      — Redis Stream for history / persistence
  {prefix}:{channel}:presence    — HASH  conn_id → server_label (cross-server view)
  {prefix}:{channel}:routes      — HASH  conn_id → json_filter | ""  (routing table)
  {prefix}:sub:{conn_id}:{chan}:cursor  — string  last delivered stream entry ID
"""

import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional

from cy_redis.core.cy_redis_client cimport CyRedisClient

# Fail-safe ceiling on how many historical stream entries a single rewind /
# resume request may pull back. Even an unbounded "(since" XRANGE is capped at
# this many entries so a replay can never fan out without limit.
DEF _MAX_HISTORY_ENTRIES = 10000


# ---------------------------------------------------------------------------
# Lua routing function loaded on CyChannelManager.start()
# Routes a message to all matching subscribers in one Redis call.
# Returns the list of conn_ids that match the filter (or all if no filter).
# ---------------------------------------------------------------------------
_ROUTE_LUA = """\
#!lua name=cy_channel

redis.register_function('cy_channel_route', function(keys, args)
    -- keys[1] = routes_key  (HASH: conn_id -> json_filter_or_empty)
    -- args[1] = json message payload
    local routes = redis.call('HGETALL', keys[1])
    if #routes == 0 then return {} end

    local ok, msg = pcall(cjson.decode, args[1])
    if not ok then return {} end

    -- msg.data contains the user payload; match filters against that
    local data = msg.data or msg

    local matches = {}
    for i = 1, #routes, 2 do
        local conn_id   = routes[i]
        local filter_js = routes[i + 1]
        if filter_js == '' or filter_js == nil then
            -- no filter — always deliver
            table.insert(matches, conn_id)
        else
            local fok, filt = pcall(cjson.decode, filter_js)
            if fok then
                local hit = true
                for k, v in pairs(filt) do
                    if tostring(data[k]) ~= tostring(v) then
                        hit = false
                        break
                    end
                end
                if hit then table.insert(matches, conn_id) end
            else
                -- unparseable filter — deliver anyway (fail-open)
                table.insert(matches, conn_id)
            end
        end
    end
    return matches
end)
"""


cdef class CyChannelConnection:
    """
    Wraps an ASGI WebSocket with a stable conn_id and channel membership set.
    Supports async iteration to receive messages from the client.
    """

    def __cinit__(self, websocket, str conn_id):
        # Preconditions: a connection needs a stable id and a live socket.
        if not conn_id:
            raise ValueError("conn_id must be a non-empty string")
        if websocket is None:
            raise ValueError("websocket is required")
        self.conn_id = conn_id
        self.websocket = websocket
        self._channels = set()
        self._closed = False

    def __init__(self, websocket, str conn_id):
        self._send_lock = asyncio.Lock()

    async def send(self, str message):
        """Send a raw text message to the client."""
        async with self._send_lock:
            await self.websocket.send_text(message)

    async def send_json(self, dict data):
        """Serialise *data* as JSON and send to the client."""
        async with self._send_lock:
            await self.websocket.send_text(json.dumps(data))

    def __aiter__(self):
        # `async for` calls __aiter__ synchronously; it must be a plain
        # method returning the iterator, not a coroutine.
        return self

    async def __anext__(self):
        """Yield text messages received from the client."""
        if self._closed:
            raise StopAsyncIteration
        try:
            return await self.websocket.receive_text()
        except Exception:
            self._closed = True
            raise StopAsyncIteration

    cpdef set get_channels(self):
        # Postcondition: callers receive an independent copy they may mutate.
        result = set(self._channels)
        assert result is not self._channels, "must return a defensive copy"
        return result

    def __repr__(self):
        return f"<CyChannelConnection id={self.conn_id!r} channels={self._channels!r}>"


cdef class CyChannelManager:
    """
    Distributed WebSocket channel manager backed by Redis pub/sub and Streams.

    Features
    --------
    * **Live delivery** — Redis PSUBSCRIBE fans out to all local connections
      subscribed to a matching channel.
    * **Stream rewind** — ``connect(rewind=N)`` delivers the last *N* messages
      from the channel's Redis Stream before going live.
    * **Cursor resume** — ``connect(since=stream_id)`` delivers all messages
      after *stream_id*, enabling lossless reconnects.
    * **Subscription router** — every channel keeps a Redis HASH mapping
      ``conn_id → json_filter``.  An empty filter means "receive all"; a JSON
      object (``{"symbol": "AAPL"}``) applies a server-side field-match.  The
      routing table is evaluated by a Lua Function so the match runs inside
      Redis — no extra round-trips.
    * **Live filter swizzle** — call ``set_filter(conn_id, channel, expr)``
      from any Python process to change what a subscriber receives in real time.
    """

    def __cinit__(self, redis_client,
                  str key_prefix='cy:chan',
                  int stream_maxlen=1000):
        # Preconditions: routing keys need a prefix and the stream needs a
        # positive trim length, otherwise XADD MAXLEN would be meaningless.
        if redis_client is None:
            raise ValueError("redis_client is required")
        if not key_prefix:
            raise ValueError("key_prefix must be a non-empty string")
        if stream_maxlen <= 0:
            raise ValueError("stream_maxlen must be positive")
        self._redis = redis_client
        self._key_prefix = key_prefix
        self._stream_maxlen = stream_maxlen
        self._connections = {}
        self._channel_members = {}
        self._psub_task = None
        self._running = False
        self._routing_loaded = False

    def __init__(self, redis_client,
                 str key_prefix='cy:chan',
                 int stream_maxlen=1000):
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _pubsub_key(self, str channel) -> str:
        return f"{self._key_prefix}:{channel}:events"

    def _stream_key(self, str channel) -> str:
        return f"{self._key_prefix}:{channel}:stream"

    def _presence_key(self, str channel) -> str:
        return f"{self._key_prefix}:{channel}:presence"

    def _routes_key(self, str channel) -> str:
        return f"{self._key_prefix}:{channel}:routes"

    def _sub_cursor_key(self, str conn_id, str channel) -> str:
        return f"{self._key_prefix}:sub:{conn_id}:{channel}:cursor"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Start the background PSUBSCRIBE listener and load Lua routing function."""
        if self._running:
            return
        self._running = True
        await self._ensure_routing_loaded()
        loop = asyncio.get_running_loop()
        self._psub_task = loop.create_task(self._psubscribe_listener())

    async def stop(self):
        """Cancel the listener and flush presence for all local connections."""
        self._running = False
        if self._psub_task is not None:
            self._psub_task.cancel()
            try:
                await self._psub_task
            except asyncio.CancelledError:
                pass
            self._psub_task = None

        async with self._lock:
            for conn_id, conn in list(self._connections.items()):
                for channel in list(conn.get_channels()):
                    await self._remove_presence(conn_id, channel)
                    await self._remove_route(conn_id, channel)
            self._connections.clear()
            self._channel_members.clear()

    async def _ensure_routing_loaded(self):
        if self._routing_loaded:
            return
        loop = asyncio.get_running_loop()

        def _load():
            try:
                self._redis.execute_command(['FUNCTION', 'LOAD', _ROUTE_LUA])
            except Exception as exc:
                err = str(exc)
                # Already loaded in this Redis instance — that is fine
                if 'already exists' not in err.lower() and 'library already exists' not in err.lower():
                    raise

        await loop.run_in_executor(None, _load)
        self._routing_loaded = True

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self, websocket, str channel,
                      conn_id=None,
                      rewind=None,
                      since=None,
                      token=None,
                      filter_expr=None) -> 'CyChannelConnection':
        """
        Accept *websocket*, register it as a subscriber of *channel*, and
        return a :class:`CyChannelConnection`.

        Parameters
        ----------
        rewind:
            Replay the last *N* historical messages from the channel's stream
            before handing over to live delivery.
        since:
            Replay all messages with stream ID **strictly after** *since*
            (use the cursor returned from a previous session for lossless
            reconnect).
        filter_expr:
            A ``dict`` of field→value pairs stored in Redis.  Only messages
            whose ``data`` payload matches **all** pairs are delivered to this
            connection.  Pass ``None`` (default) to receive everything.
        """
        # Precondition: a connection must join a named channel.
        if not channel:
            raise ValueError("channel must be a non-empty string")

        await websocket.accept()

        if conn_id is None:
            conn_id = str(uuid.uuid4())
        assert conn_id, "conn_id must be resolved to a non-empty value"

        conn = CyChannelConnection(websocket, conn_id)

        async with self._lock:
            self._connections[conn_id] = conn

        await self._subscribe_internal(conn_id, channel, filter_expr)

        # Replay history before live messages start flowing
        if rewind is not None or since is not None:
            # Resolve a persisted cursor if the caller didn't provide one
            if since is None and rewind is None:
                since = await self._load_cursor(conn_id, channel)
            await self._replay_history(conn, channel, rewind, since)

        return conn

    async def disconnect(self, conn: 'CyChannelConnection'):
        """Unsubscribe *conn* from all channels and remove from local state."""
        for channel in list(conn.get_channels()):
            await self._unsubscribe_internal(conn.conn_id, channel)

        async with self._lock:
            self._connections.pop(conn.conn_id, None)

        conn._closed = True

    # ------------------------------------------------------------------
    # Channel operations
    # ------------------------------------------------------------------

    async def subscribe(self, str conn_id, str channel, filter_expr=None):
        """Subscribe an existing connection to an additional channel."""
        await self._subscribe_internal(conn_id, channel, filter_expr)

    async def unsubscribe(self, str conn_id, str channel):
        """Unsubscribe a connection from a channel."""
        await self._unsubscribe_internal(conn_id, channel)

    async def set_filter(self, str conn_id, str channel, filter_expr):
        """
        Update the server-side filter for *conn_id* on *channel* in place.

        Can be called from **any** Python process — the change is written to
        Redis and takes effect immediately for the next routed message.

        Parameters
        ----------
        filter_expr:
            ``dict`` of field→value pairs, or ``None`` / ``{}`` to remove the
            filter and receive all messages.
        """
        loop = asyncio.get_running_loop()
        routes_key = self._routes_key(channel)
        raw_filter = json.dumps(filter_expr) if filter_expr else ''
        await loop.run_in_executor(
            None,
            lambda: self._redis.execute_command(['HSET', routes_key, conn_id, raw_filter])
        )

    async def publish(self, str channel, dict data,
                      sender_conn=None,
                      bint exclude_sender=False):
        """
        Publish *data* to *channel*.

        The message is:
        1. Appended to the channel's Redis Stream (for history / rewind).
        2. Routed to subscribers via the Lua ``cy_channel_route`` function
           (server-side filter matching).
        3. Published to the Redis pub/sub channel so all manager instances
           deliver to their local connections.
        """
        # Precondition: a publish must target a named channel.
        if not channel:
            raise ValueError("channel must be a non-empty string")

        envelope = {
            'event': 'message',
            'channel': channel,
            'sender': sender_conn or '',
            'exclude_sender': exclude_sender,
            'data': data,
        }
        raw = json.dumps(envelope)
        assert raw, "serialised envelope must be non-empty"
        loop = asyncio.get_running_loop()
        stream_key = self._stream_key(channel)
        pubsub_key = self._pubsub_key(channel)
        maxlen = self._stream_maxlen

        def _publish_sync():
            # Persist to stream first to get a canonical entry ID
            entry_id = self._redis.execute_command([
                'XADD', stream_key,
                'MAXLEN', '~', str(maxlen),
                '*',
                'payload', raw,
            ])
            if isinstance(entry_id, bytes):
                entry_id = entry_id.decode()
            envelope['ts'] = entry_id
            final_raw = json.dumps(envelope)
            # Broadcast via pub/sub — all manager instances pick this up
            self._redis.publish(pubsub_key, final_raw)
            return entry_id

        await loop.run_in_executor(None, _publish_sync)

    async def emit(self, str conn_id, dict data):
        """Send *data* directly to a single connection (not broadcast)."""
        async with self._lock:
            conn = self._connections.get(conn_id)
        if conn is not None:
            await conn.send_json(data)

    # ------------------------------------------------------------------
    # Stream history / rewind
    # ------------------------------------------------------------------

    async def history(self, str channel,
                      count=None,
                      since=None) -> List[Dict]:
        """
        Fetch historical messages from the channel's Redis Stream.

        Parameters
        ----------
        count:
            Maximum number of messages to return.  When *since* is ``None``
            the *count* most recent messages are returned (XREVRANGE).
        since:
            Return only messages with stream ID **strictly after** *since*
            (exclusive, using Redis ``(id`` notation).  Ignores *count*.
        """
        # Precondition: an explicit count must be a sane positive request.
        if count is not None and count <= 0:
            raise ValueError("count must be positive when provided")

        loop = asyncio.get_running_loop()
        stream_key = self._stream_key(channel)

        def _history_sync():
            if since is not None:
                # Exclusive range start: "(since" returns entries after *since*.
                # Cap the reply with COUNT so even a far-back cursor cannot pull
                # an unbounded number of entries in one shot.
                result = self._redis.execute_command(
                    ['XRANGE', stream_key, f'({since}', '+',
                     'COUNT', str(_MAX_HISTORY_ENTRIES)]
                )
            else:
                requested = count if count is not None else 50
                # Bound the rewind window to the fail-safe ceiling.
                bounded_count = min(requested, _MAX_HISTORY_ENTRIES)
                assert 0 < bounded_count <= _MAX_HISTORY_ENTRIES, \
                    "history count must stay within the fail-safe ceiling"
                result = self._redis.execute_command(
                    ['XREVRANGE', stream_key, '+', '-', 'COUNT', str(bounded_count)]
                )
                if result:
                    result = list(reversed(result))
            return result or []

        raw = await loop.run_in_executor(None, _history_sync)
        messages = self._parse_stream_entries(raw)
        # Postcondition: never hand back more than the fail-safe ceiling.
        assert len(messages) <= _MAX_HISTORY_ENTRIES, \
            "parsed history must respect the fail-safe ceiling"
        return messages

    def _parse_stream_entries(self, raw_entries) -> List[Dict]:
        messages = []
        # Bounded by the number of stream entries Redis returned, which the
        # caller (history) already caps at _MAX_HISTORY_ENTRIES via COUNT.
        for entry in raw_entries:
            if not entry or len(entry) < 2:
                continue
            entry_id = entry[0]
            if isinstance(entry_id, bytes):
                entry_id = entry_id.decode()
            fields = entry[1]
            for i in range(0, len(fields) - 1, 2):
                fname = fields[i]
                if fname in (b'payload', 'payload'):
                    raw = fields[i + 1]
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    try:
                        msg = json.loads(raw)
                        msg['ts'] = entry_id
                        messages.append(msg)
                    except Exception:
                        pass
                    break
        return messages

    # ------------------------------------------------------------------
    # Presence
    # ------------------------------------------------------------------

    async def get_members(self, str channel) -> List[str]:
        """Return all conn_ids subscribed to *channel* (cross-server view)."""
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self._redis.execute_command(
                    ['HKEYS', self._presence_key(channel)]
                )
            )
            if not result:
                return []
            return [k.decode() if isinstance(k, bytes) else k for k in result]
        except Exception:
            return []

    async def channel_count(self, str channel) -> int:
        """Return the number of subscribers on *channel* (cross-server)."""
        loop = asyncio.get_running_loop()
        try:
            n = await loop.run_in_executor(
                None,
                lambda: self._redis.execute_command(
                    ['HLEN', self._presence_key(channel)]
                )
            )
            return n or 0
        except Exception:
            return 0

    async def get_subscriptions(self, str conn_id) -> List[str]:
        """Return the channels *conn_id* is locally subscribed to."""
        async with self._lock:
            conn = self._connections.get(conn_id)
            return list(conn.get_channels()) if conn is not None else []

    # ------------------------------------------------------------------
    # Cursor persistence (lossless reconnect)
    # ------------------------------------------------------------------

    async def save_cursor(self, str conn_id, str channel, str stream_id):
        """
        Persist *stream_id* as the last-delivered message cursor for this
        (conn_id, channel) pair.  Call after successfully delivering a message
        to enable lossless reconnect via ``connect(since=cursor)``.
        """
        loop = asyncio.get_running_loop()
        key = self._sub_cursor_key(conn_id, channel)
        await loop.run_in_executor(
            None,
            lambda: self._redis.execute_command(['SET', key, stream_id, 'EX', '604800'])
        )

    async def _load_cursor(self, str conn_id, str channel) -> Optional[str]:
        loop = asyncio.get_running_loop()
        key = self._sub_cursor_key(conn_id, channel)
        try:
            val = await loop.run_in_executor(
                None,
                lambda: self._redis.execute_command(['GET', key])
            )
            if val is None:
                return None
            return val.decode() if isinstance(val, bytes) else val
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _subscribe_internal(self, str conn_id, str channel, filter_expr):
        async with self._lock:
            if channel not in self._channel_members:
                self._channel_members[channel] = set()
            self._channel_members[channel].add(conn_id)
            conn = self._connections.get(conn_id)
            if conn is not None:
                conn._channels.add(channel)

        # Persist presence and routing filter in Redis
        loop = asyncio.get_running_loop()
        presence_key = self._presence_key(channel)
        routes_key = self._routes_key(channel)
        raw_filter = json.dumps(filter_expr) if filter_expr else ''

        def _register():
            self._redis.execute_command(['HSET', presence_key, conn_id, 'local'])
            self._redis.execute_command(['HSET', routes_key, conn_id, raw_filter])

        await loop.run_in_executor(None, _register)

    async def _unsubscribe_internal(self, str conn_id, str channel):
        async with self._lock:
            members = self._channel_members.get(channel)
            if members:
                members.discard(conn_id)
                if not members:
                    del self._channel_members[channel]
            conn = self._connections.get(conn_id)
            if conn is not None:
                conn._channels.discard(channel)

        await self._remove_presence(conn_id, channel)
        await self._remove_route(conn_id, channel)

    async def _remove_presence(self, str conn_id, str channel):
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._redis.execute_command(
                    ['HDEL', self._presence_key(channel), conn_id]
                )
            )
        except Exception:
            pass

    async def _remove_route(self, str conn_id, str channel):
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._redis.execute_command(
                    ['HDEL', self._routes_key(channel), conn_id]
                )
            )
        except Exception:
            pass

    async def _replay_history(self, conn: 'CyChannelConnection',
                               str channel, rewind, since):
        assert conn is not None, "replay target connection must exist"
        entries = await self.history(channel, count=rewind, since=since)
        # history() caps its result at _MAX_HISTORY_ENTRIES, so this delivery
        # loop is bounded by that same ceiling.
        assert len(entries) <= _MAX_HISTORY_ENTRIES, \
            "replay must not exceed the history ceiling"
        for entry in entries:
            try:
                await conn.send_json(entry)
                # Update cursor as we deliver
                if 'ts' in entry:
                    await self.save_cursor(conn.conn_id, channel, entry['ts'])
            except Exception:
                break

    # ------------------------------------------------------------------
    # PSUBSCRIBE listener (one task per manager instance)
    # ------------------------------------------------------------------

    async def _psubscribe_listener(self):
        """
        Long-running asyncio task.  Subscribes to ``{prefix}:*:events`` via
        PSUBSCRIBE and delivers incoming messages to local connections.

        Uses Lua ``cy_channel_route`` to evaluate per-subscriber filters
        entirely inside Redis, then delivers only to matching local conn_ids.
        """
        from cy_redis.utils.redis_iterators import RedisPSubIterator

        pattern = f"{self._key_prefix}:*:events"
        prefix_len = len(self._key_prefix) + 1   # "cy:chan:" chars
        suffix_len = len(":events")
        # Invariant: the slice bounds below assume both affixes are present.
        assert prefix_len > 0 and suffix_len > 0, "channel affix lengths"

        iterator = RedisPSubIterator(self._redis, pattern, timeout_ms=500)
        # Exit invariant: the loop runs only while self._running is True; stop()
        # clears the flag and cancels the task, and the iterator is always
        # closed in the finally clause so its connection cannot leak.
        try:
            async for msg in iterator:
                if not self._running:
                    break
                if msg is None:
                    # timeout — keep looping
                    continue
                chan_key = msg.get('channel', '')
                # Extract channel name from "cy:chan:{name}:events"
                if (chan_key.startswith(self._key_prefix + ':')
                        and chan_key.endswith(':events')):
                    channel = chan_key[prefix_len:-suffix_len]
                    await self._deliver_local(channel, msg.get('data', ''))
        except asyncio.CancelledError:
            pass
        finally:
            iterator.close()

    async def _deliver_local(self, str channel, str raw_message):
        """
        Deliver *raw_message* to all locally-connected subscribers of *channel*
        that pass their filter.

        When a ``cy_channel_route`` Lua function is available the routing
        decision is delegated to Redis (evaluates all filters in one call).
        Otherwise falls back to delivering to every local subscriber.
        """
        if not raw_message:
            return

        # Ask Redis which conn_ids match their filters
        matching = await self._route_via_lua(channel, raw_message)

        if matching is None:
            # Lua unavailable — deliver to all local subscribers
            async with self._lock:
                matching = list(self._channel_members.get(channel, set()))

        # Decode sender and cursor from envelope
        sender = ''
        exclude = False
        ts = None
        try:
            envelope = json.loads(raw_message)
            sender = envelope.get('sender', '')
            exclude = bool(envelope.get('exclude_sender', False))
            ts = envelope.get('ts')
        except Exception:
            envelope = None

        async with self._lock:
            conn_map = {
                cid: self._connections[cid]
                for cid in matching
                if cid in self._connections
            }

        # Bounded by the number of locally-connected matches; can never exceed
        # the count of live connections on this manager instance.
        assert len(conn_map) <= len(self._connections), \
            "delivery set cannot exceed live connection count"
        for conn_id, conn in conn_map.items():
            if exclude and conn_id == sender:
                continue
            try:
                await conn.send(raw_message)
                if ts:
                    asyncio.ensure_future(
                        self.save_cursor(conn_id, channel, ts)
                    )
            except Exception:
                pass

    async def _route_via_lua(self, str channel, str raw_message):
        """
        Call the ``cy_channel_route`` Lua Function in Redis to determine
        which conn_ids on *channel* should receive this message.

        Returns a list of conn_id strings, or ``None`` if the call fails.
        """
        if not self._routing_loaded:
            return None

        loop = asyncio.get_running_loop()
        routes_key = self._routes_key(channel)

        def _call():
            result = self._redis.execute_command(
                ['FCALL', 'cy_channel_route', '1', routes_key, raw_message]
            )
            if result is None:
                return []
            return [
                r.decode() if isinstance(r, bytes) else r
                for r in result
            ]

        try:
            return await loop.run_in_executor(None, _call)
        except Exception:
            return None
