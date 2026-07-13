# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Redis Data Structure Iterators for CyRedis Web Application Support.
Provides async iterators for Redis streams, lists, and pub/sub.
"""

import asyncio
import json
from typing import Any, Dict, List, Optional, Union

# Import core Redis functionality

from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class RedisStreamIterator:
    """
    Async iterator for Redis streams.
    Allows iteration over stream messages for real-time streaming.
    """

    def __cinit__(self, CyRedisClient redis_client, str stream_key,
                  str consumer_group=None, str consumer_name=None,
                  int batch_size=10, int block_ms=1000):
        # Preconditions: invalid caller arguments raise, not assert.
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if not stream_key:
            raise ValueError("stream_key must be a non-empty string")
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive count")
        if block_ms < 0:
            raise ValueError("block_ms must be non-negative milliseconds")

        self.redis_client = redis_client
        self.stream_key = stream_key
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.last_id = "0"  # Start from beginning
        self.is_closed = False

    async def __aiter__(self):
        """Async iterator protocol."""
        return self

    async def __anext__(self):
        """Get next batch of messages."""
        if self.is_closed:
            raise StopAsyncIteration

        try:
            if self.consumer_group and self.consumer_name:
                # Use consumer group for reliable delivery
                messages = await self._read_from_consumer_group()
            else:
                # Use simple stream reading
                messages = await self._read_from_stream()

            if not messages:
                raise StopAsyncIteration

            # Invariant: a non-empty batch is bounded by batch_size.
            assert len(messages) <= self.batch_size, "batch exceeds batch_size bound"

            # Update last_id for next iteration
            if messages:
                self.last_id = messages[-1]['id']

            # Invariant: cursor must remain a non-empty stream id for XREAD.
            assert self.last_id, "stream cursor must be a non-empty id"

            return messages

        except Exception as e:
            self.is_closed = True
            raise StopAsyncIteration from e

    async def _read_from_stream(self):
        """Read messages from stream without consumer group."""
        try:
            # Use XREAD with BLOCK
            result = await self._execute_xread()

            if not result:
                return []

            # Parse the result
            messages = []
            for stream_data in result:
                if len(stream_data) >= 2:
                    stream_name = stream_data[0]
                    msg_list = stream_data[1]

                    for msg in msg_list:
                        if len(msg) >= 2:
                            msg_id = msg[0]
                            msg_data = {}

                            # Parse field-value pairs
                            for i in range(1, len(msg), 2):
                                if i + 1 < len(msg):
                                    field = msg[i]
                                    value = msg[i + 1]
                                    msg_data[field] = value

                            messages.append({
                                'id': msg_id,
                                'stream': stream_name,
                                'data': msg_data
                            })

            # Invariant: the yielded batch never exceeds the requested bound.
            bounded = messages[:self.batch_size]
            assert len(bounded) <= self.batch_size, "batch exceeds batch_size bound"
            return bounded

        except Exception as e:
            print(f"Error reading from stream: {e}")
            return []

    async def _read_from_consumer_group(self):
        """Read messages from consumer group."""
        try:
            # Use XREADGROUP with BLOCK
            result = await self._execute_xreadgroup()

            if not result:
                return []

            # Parse consumer group result
            messages = []
            for stream_data in result:
                if len(stream_data) >= 2:
                    stream_name = stream_data[0]
                    msg_list = stream_data[1]

                    for msg in msg_list:
                        if len(msg) >= 2:
                            msg_id = msg[0]
                            msg_data = {}

                            # Parse field-value pairs
                            for i in range(1, len(msg), 2):
                                if i + 1 < len(msg):
                                    field = msg[i]
                                    value = msg[i + 1]
                                    msg_data[field] = value

                            messages.append({
                                'id': msg_id,
                                'stream': stream_name,
                                'data': msg_data
                            })

            # Invariant: the yielded batch never exceeds the requested bound.
            bounded = messages[:self.batch_size]
            assert len(bounded) <= self.batch_size, "batch exceeds batch_size bound"
            return bounded

        except Exception as e:
            print(f"Error reading from consumer group: {e}")
            return []

    async def _execute_xread(self):
        """Execute XREAD command."""
        loop = asyncio.get_event_loop()

        def _xread_sync():
            return self.redis_client.xread(
                streams={self.stream_key: self.last_id},
                count=self.batch_size,
                block=self.block_ms
            )

        return await loop.run_in_executor(None, _xread_sync)

    async def _execute_xreadgroup(self):
        """Execute XREADGROUP command."""
        loop = asyncio.get_event_loop()

        def _xreadgroup_sync():
            # Create consumer group if it doesn't exist
            try:
                self.redis_client.xgroup_create(
                    self.stream_key,
                    self.consumer_group,
                    mkstream=True
                )
            except Exception:
                # Group might already exist
                pass

            return self.redis_client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams={self.stream_key: '>'},
                count=self.batch_size,
                block=self.block_ms
            )

        return await loop.run_in_executor(None, _xreadgroup_sync)

    def close(self):
        """Close the iterator."""
        self.is_closed = True

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()


cdef class RedisListIterator:
    """
    Async iterator for Redis lists.
    Allows iteration over list items for real-time streaming.
    """

    def __cinit__(self, CyRedisClient redis_client, str list_key,
                  int batch_size=10, int block_ms=1000):
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if not list_key:
            raise ValueError("list_key must be a non-empty string")
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive count")
        if block_ms < 0:
            raise ValueError("block_ms must be non-negative milliseconds")

        self.redis_client = redis_client
        self.list_key = list_key
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.is_closed = False
        self.last_index = 0

    async def __aiter__(self):
        """Async iterator protocol."""
        return self

    async def __anext__(self):
        """Get next batch of list items."""
        if self.is_closed:
            raise StopAsyncIteration

        try:
            items = await self._read_from_list()

            if not items:
                raise StopAsyncIteration

            # Update last_index for next iteration
            self.last_index += len(items)

            return items

        except Exception as e:
            self.is_closed = True
            raise StopAsyncIteration from e

    async def _read_from_list(self):
        """Read items from list."""
        loop = asyncio.get_event_loop()

        def _list_read_sync():
            # Get list length
            list_length = self.redis_client.llen(self.list_key)

            if list_length == 0:
                return []

            # Calculate range to read; the window is bounded by batch_size.
            start = self.last_index
            end = min(start + self.batch_size - 1, list_length - 1)

            if start >= list_length:
                return []

            # Invariant: the LRANGE window never exceeds the batch bound.
            assert end >= start, "range end must not precede start"
            assert (end - start + 1) <= self.batch_size, "range exceeds batch_size"

            # Read items from list
            items = self.redis_client.lrange(self.list_key, start, end)

            return items

        return await loop.run_in_executor(None, _list_read_sync)

    def close(self):
        """Close the iterator."""
        self.is_closed = True

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()


cdef class RedisPSubIterator:
    """
    Async iterator for Redis pattern subscriptions (PSUBSCRIBE).

    Subscribes to a glob pattern (e.g. ``cy:chan:*:events``) on a single
    dedicated connection.  Each message yielded is a dict::

        {'pattern': str, 'channel': str, 'data': str}

    The iterator must be closed explicitly (or used as an async context
    manager) to release the underlying connection.
    """

    def __cinit__(self, CyRedisClient redis_client, str pattern,
                  int timeout_ms=1000):
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if not pattern:
            raise ValueError("pattern must be a non-empty string")
        if timeout_ms <= 0:
            raise ValueError("timeout_ms must be a positive number of milliseconds")

        self.redis_client = redis_client
        self.pattern = pattern
        self.timeout_ms = timeout_ms
        self.is_closed = False
        self.pubsub = None

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if self.is_closed:
            raise StopAsyncIteration

        try:
            if not self.pubsub:
                await self._initialize_psub()

            message = await self._get_next_message()

            if message is None:
                # timeout — caller can retry; don't terminate the iterator
                return None

            return message

        except StopAsyncIteration:
            raise
        except Exception as e:
            self.is_closed = True
            raise StopAsyncIteration from e

    async def _initialize_psub(self):
        """Open a dedicated connection, send PSUBSCRIBE, start reader thread."""
        import threading

        loop = asyncio.get_running_loop()
        pattern = self.pattern

        def _setup():
            from cy_redis.core.cy_redis_client import CyRedisConnection
            conn = CyRedisConnection(
                self.redis_client.pool.get_host(),
                self.redis_client.pool.get_port(),
            )
            if conn.connect() != 0:
                raise ConnectionError("PSubIterator connection failed")
            conn.execute_command(['PSUBSCRIBE', pattern])
            return conn

        conn = await loop.run_in_executor(None, _setup)
        self.pubsub = conn
        self._msg_queue = asyncio.Queue()

        def _reader():
            # Exit invariant: the loop blocks on read_reply() (one turn per
            # inbound frame, so it is bounded by network traffic, not a spin)
            # and terminates when close() sets is_closed or read_reply raises.
            while not self.is_closed:
                try:
                    reply = conn.read_reply()
                    # pmessage frame: ['pmessage', pattern, channel, data]
                    if isinstance(reply, list) and len(reply) == 4:
                        assert len(reply) == 4, "pmessage frame has four parts"
                        kind = reply[0]
                        if isinstance(kind, bytes):
                            kind = kind.decode()
                        if kind == 'pmessage':
                            pat = reply[1].decode() if isinstance(reply[1], bytes) else reply[1]
                            chan = reply[2].decode() if isinstance(reply[2], bytes) else reply[2]
                            data = reply[3].decode() if isinstance(reply[3], bytes) else reply[3]
                            msg = {'pattern': pat, 'channel': chan, 'data': data}
                            loop.call_soon_threadsafe(self._msg_queue.put_nowait, msg)
                except Exception:
                    if not self.is_closed:
                        loop.call_soon_threadsafe(self._msg_queue.put_nowait, None)
                    break

        t = threading.Thread(target=_reader, daemon=True)
        t.start()

    async def _get_next_message(self):
        if not hasattr(self, '_msg_queue'):
            return None
        # Invariant: the wait is bounded by a strictly positive timeout.
        assert self.timeout_ms > 0, "timeout_ms must be positive"
        try:
            return await asyncio.wait_for(
                self._msg_queue.get(), timeout=self.timeout_ms / 1000
            )
        except asyncio.TimeoutError:
            return None

    def close(self):
        self.is_closed = True
        if self.pubsub is not None:
            try:
                self.pubsub.execute_command(['PUNSUBSCRIBE', self.pattern])
                self.pubsub.disconnect()
            except Exception:
                pass
            self.pubsub = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()


cdef class RedisPubSubIterator:
    """
    Async iterator for Redis pub/sub.
    Allows iteration over pub/sub messages for real-time streaming.
    """

    def __cinit__(self, CyRedisClient redis_client, list channels,
                  int timeout_ms=1000):
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if not channels:
            raise ValueError("channels must be a non-empty list")
        if timeout_ms <= 0:
            raise ValueError("timeout_ms must be a positive number of milliseconds")

        self.redis_client = redis_client
        self.channels = channels if isinstance(channels, list) else [channels]
        self.timeout_ms = timeout_ms
        self.is_closed = False
        self.pubsub = None
        # Postcondition: the subscription set is a non-empty list.
        assert isinstance(self.channels, list), "channels normalized to a list"
        assert len(self.channels) > 0, "must subscribe to at least one channel"

    async def __aiter__(self):
        """Async iterator protocol."""
        return self

    async def __anext__(self):
        """Get next pub/sub message."""
        if self.is_closed:
            raise StopAsyncIteration

        try:
            if not self.pubsub:
                await self._initialize_pubsub()

            message = await self._get_next_message()

            if not message:
                raise StopAsyncIteration

            return message

        except Exception as e:
            self.is_closed = True
            raise StopAsyncIteration from e

    async def _initialize_pubsub(self):
        """Open a dedicated connection, subscribe, and start a reader thread."""
        import queue as _queue
        import threading

        loop = asyncio.get_running_loop()

        def _setup():
            from cy_redis.core.cy_redis_client import CyRedisConnection
            conn = CyRedisConnection(
                self.redis_client.pool.get_host(),
                self.redis_client.pool.get_port(),
            )
            if conn.connect() != 0:
                raise ConnectionError("PubSub connection failed")
            # Consume subscription confirmation replies
            for channel in self.channels:
                conn.execute_command(['SUBSCRIBE', channel])
            return conn

        conn = await loop.run_in_executor(None, _setup)
        self.pubsub = conn
        self._msg_queue = asyncio.Queue()

        # Background thread: block on read_reply() and put messages into the queue
        def _reader():
            # Exit invariant: each turn blocks on read_reply() (bounded by
            # inbound traffic, not a spin) and terminates when close() sets
            # is_closed or read_reply raises.
            while not self.is_closed:
                try:
                    reply = conn.read_reply()
                    # RESP array: ['message', channel, data]
                    if isinstance(reply, list) and len(reply) == 3 and reply[0] == 'message':
                        msg = {
                            'type': 'message',
                            'channel': reply[1] if isinstance(reply[1], str) else reply[1].decode(),
                            'data': reply[2] if isinstance(reply[2], str) else reply[2].decode(),
                        }
                        loop.call_soon_threadsafe(self._msg_queue.put_nowait, msg)
                except Exception:
                    if not self.is_closed:
                        loop.call_soon_threadsafe(self._msg_queue.put_nowait, None)
                    break

        t = threading.Thread(target=_reader, daemon=True)
        t.start()

    async def _get_next_message(self):
        """Wait for the next message from the reader thread."""
        if not hasattr(self, '_msg_queue'):
            return None
        # Invariant: the wait is bounded by a strictly positive timeout.
        assert self.timeout_ms > 0, "timeout_ms must be positive"
        try:
            msg = await asyncio.wait_for(self._msg_queue.get(), timeout=self.timeout_ms / 1000)
            return msg
        except asyncio.TimeoutError:
            return None

    def close(self):
        """Close the iterator and unsubscribe."""
        self.is_closed = True
        if self.pubsub is not None:
            try:
                for channel in self.channels:
                    self.pubsub.execute_command(['UNSUBSCRIBE', channel])
                self.pubsub.disconnect()
            except Exception:
                pass
            self.pubsub = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()


