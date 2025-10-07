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
from typing import Dict, List, Union, Optional, Any

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

            # Update last_id for next iteration
            if messages:
                self.last_id = messages[-1]['id']

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

            return messages[:self.batch_size]

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

            return messages[:self.batch_size]

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

            # Calculate range to read
            start = self.last_index
            end = min(start + self.batch_size - 1, list_length - 1)

            if start >= list_length:
                return []

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


cdef class RedisPubSubIterator:
    """
    Async iterator for Redis pub/sub.
    Allows iteration over pub/sub messages for real-time streaming.
    """

    def __cinit__(self, CyRedisClient redis_client, list channels,
                  int timeout_ms=1000):
        self.redis_client = redis_client
        self.channels = channels if isinstance(channels, list) else [channels]
        self.timeout_ms = timeout_ms
        self.is_closed = False
        self.pubsub = None

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
        """Initialize pub/sub connection."""
        loop = asyncio.get_event_loop()

        def _init_pubsub():
            # Import redis here to avoid issues in Cython context
            import redis
            self.pubsub = self.redis_client.pubsub()

            # Subscribe to channels
            for channel in self.channels:
                self.pubsub.subscribe(channel)

            return True

        await loop.run_in_executor(None, _init_pubsub)

    async def _get_next_message(self):
        """Get next message from pub/sub."""
        loop = asyncio.get_event_loop()

        def _get_message():
            if not self.pubsub:
                return None

            # Get message with timeout
            message = self.pubsub.get_message(timeout=self.timeout_ms / 1000)

            if message and message['type'] == 'message':
                return {
                    'type': 'message',
                    'channel': message['channel'].decode() if isinstance(message['channel'], bytes) else message['channel'],
                    'data': message['data'].decode() if isinstance(message['data'], bytes) else message['data']
                }

            return None

        return await loop.run_in_executor(None, _get_message)

    def close(self):
        """Close the iterator."""
        self.is_closed = True
        if self.pubsub:
            try:
                self.pubsub.close()
            except Exception:
                pass
            self.pubsub = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.close()


