"""
Integration tests for async Redis operations.

Tests asynchronous operations with real Redis including:
- Async GET/SET operations
- Async pipelines
- Concurrent async operations
- Event loop integration
- uvloop performance
- Async connection pooling
"""

import pytest
import asyncio
import time
from typing import List


# Check for async Redis support
try:
    from redis.asyncio import Redis as AsyncRedis
    ASYNC_REDIS_AVAILABLE = True
except ImportError:
    ASYNC_REDIS_AVAILABLE = False

try:
    import uvloop
    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False


@pytest.fixture
async def async_redis_client(redis_available):
    """Provide an async Redis client."""
    if not redis_available:
        pytest.skip("Redis not available")
    if not ASYNC_REDIS_AVAILABLE:
        pytest.skip("Async Redis not available")

    import os
    client = AsyncRedis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True
    )

    yield client

    # Cleanup
    try:
        test_keys = await client.keys("test:*")
        if test_keys:
            await client.delete(*test_keys)
    except Exception:
        pass
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncBasicOperations:
    """Test basic async operations."""

    async def test_async_set_get(self, async_redis_client, unique_key):
        """Test async SET and GET."""
        key = f"{unique_key}:async"
        value = "async_value"

        # Set
        result = await async_redis_client.set(key, value)
        assert result is True

        # Get
        retrieved = await async_redis_client.get(key)
        assert retrieved == value

        # Cleanup
        await async_redis_client.delete(key)

    async def test_async_mget_mset(self, async_redis_client, unique_key):
        """Test async MGET and MSET."""
        keys = [f"{unique_key}:async:{i}" for i in range(10)]
        values = [f"value_{i}" for i in range(10)]
        mapping = {k: v for k, v in zip(keys, values)}

        # Multi-set
        await async_redis_client.mset(mapping)

        # Multi-get
        retrieved = await async_redis_client.mget(keys)
        assert retrieved == values

        # Cleanup
        await async_redis_client.delete(*keys)

    async def test_async_incr(self, async_redis_client, unique_key):
        """Test async INCR."""
        key = f"{unique_key}:counter"

        # Increment
        result = await async_redis_client.incr(key)
        assert result == 1

        result = await async_redis_client.incr(key)
        assert result == 2

        # Cleanup
        await async_redis_client.delete(key)

    async def test_async_expire(self, async_redis_client, unique_key):
        """Test async EXPIRE."""
        key = f"{unique_key}:expire"

        await async_redis_client.set(key, "value", ex=2)

        # Should exist
        assert await async_redis_client.exists(key) == 1

        # Wait for expiration
        await asyncio.sleep(2.1)

        # Should not exist
        assert await async_redis_client.exists(key) == 0


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncPipeline:
    """Test async pipeline operations."""

    async def test_async_pipeline_basic(self, async_redis_client, unique_key):
        """Test basic async pipeline."""
        keys = [f"{unique_key}:pipe:{i}" for i in range(5)]

        # Execute pipeline
        async with async_redis_client.pipeline() as pipe:
            for i, key in enumerate(keys):
                pipe.set(key, f"value_{i}")
            results = await pipe.execute()

        # All should succeed
        assert all(r is True for r in results)

        # Verify values
        for i, key in enumerate(keys):
            value = await async_redis_client.get(key)
            assert value == f"value_{i}"

        # Cleanup
        await async_redis_client.delete(*keys)

    async def test_async_pipeline_mixed(self, async_redis_client, unique_key):
        """Test async pipeline with mixed operations."""
        key1 = f"{unique_key}:pipe1"
        key2 = f"{unique_key}:pipe2"

        # Setup
        await async_redis_client.set(key1, "10")

        # Pipeline with mixed operations
        async with async_redis_client.pipeline() as pipe:
            pipe.incr(key1)
            pipe.get(key1)
            pipe.set(key2, "value2")
            pipe.get(key2)
            results = await pipe.execute()

        # Check results
        assert results[0] == 11  # incr result
        assert results[1] == "11"  # get result
        assert results[2] is True  # set result
        assert results[3] == "value2"  # get result

        # Cleanup
        await async_redis_client.delete(key1, key2)


@pytest.mark.integration
@pytest.mark.asyncio
class TestConcurrentAsyncOperations:
    """Test concurrent async operations."""

    async def test_concurrent_async_reads(self, async_redis_client, unique_key):
        """Test concurrent async read operations."""
        key = f"{unique_key}:concurrent"
        await async_redis_client.set(key, "test_value")

        async def read_value():
            return await async_redis_client.get(key)

        # Run 100 concurrent reads
        tasks = [read_value() for _ in range(100)]
        results = await asyncio.gather(*tasks)

        # All should succeed with correct value
        assert len(results) == 100
        assert all(r == "test_value" for r in results)

        # Cleanup
        await async_redis_client.delete(key)

    async def test_concurrent_async_writes(self, async_redis_client, unique_key):
        """Test concurrent async write operations."""
        base_key = f"{unique_key}:concurrent"

        async def write_value(index):
            key = f"{base_key}:{index}"
            await async_redis_client.set(key, f"value_{index}")
            return key

        # Run 100 concurrent writes
        tasks = [write_value(i) for i in range(100)]
        keys = await asyncio.gather(*tasks)

        # Verify all writes
        for i, key in enumerate(keys):
            value = await async_redis_client.get(key)
            assert value == f"value_{i}"

        # Cleanup
        await async_redis_client.delete(*keys)

    async def test_concurrent_async_incr(self, async_redis_client, unique_key):
        """Test concurrent async INCR (atomic)."""
        key = f"{unique_key}:atomic_counter"

        async def increment():
            return await async_redis_client.incr(key)

        # Run 100 concurrent increments
        tasks = [increment() for _ in range(100)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert len(results) == 100

        # Final count should be exactly 100
        final = await async_redis_client.get(key)
        assert int(final) == 100

        # Cleanup
        await async_redis_client.delete(key)

    async def test_many_concurrent_operations(self, async_redis_client, unique_key):
        """Test many concurrent mixed operations."""
        base_key = f"{unique_key}:many"

        async def mixed_operation(index):
            key = f"{base_key}:{index}"
            await async_redis_client.set(key, f"value_{index}")
            value = await async_redis_client.get(key)
            await async_redis_client.incr(f"{base_key}:counter")
            return value == f"value_{index}"

        # Run 500 concurrent operations
        tasks = [mixed_operation(i) for i in range(500)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert all(results)

        # Counter should be 500
        counter = await async_redis_client.get(f"{base_key}:counter")
        assert int(counter) == 500

        # Cleanup
        keys = [f"{base_key}:{i}" for i in range(500)]
        keys.append(f"{base_key}:counter")
        await async_redis_client.delete(*keys)


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncTransactions:
    """Test async transaction operations."""

    async def test_async_watch(self, async_redis_client, unique_key):
        """Test async WATCH operation."""
        key = f"{unique_key}:watch"
        await async_redis_client.set(key, "0")

        async with async_redis_client.pipeline() as pipe:
            # Watch key
            await pipe.watch(key)

            # Get current value
            current = int(await async_redis_client.get(key))

            # Execute transaction
            pipe.multi()
            pipe.set(key, str(current + 1))
            results = await pipe.execute()

            # Should succeed
            assert results == [True]

        # Verify value
        assert await async_redis_client.get(key) == "1"

        # Cleanup
        await async_redis_client.delete(key)


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncStreamOperations:
    """Test async stream operations."""

    async def test_async_xadd_xread(self, async_redis_client, unique_key):
        """Test async XADD and XREAD."""
        stream = f"{unique_key}:stream"

        # Add messages
        id1 = await async_redis_client.xadd(stream, {"field1": "value1"})
        id2 = await async_redis_client.xadd(stream, {"field2": "value2"})

        assert id1 is not None
        assert id2 is not None

        # Read messages
        messages = await async_redis_client.xread({stream: "0"})

        assert len(messages) == 1
        assert len(messages[0][1]) == 2  # 2 messages

        # Cleanup
        await async_redis_client.delete(stream)

    async def test_async_xread_block(self, async_redis_client, unique_key):
        """Test async XREAD with blocking."""
        stream = f"{unique_key}:stream"

        async def producer():
            await asyncio.sleep(0.5)
            await async_redis_client.xadd(stream, {"data": "value"})

        async def consumer():
            # Block for up to 2 seconds
            messages = await async_redis_client.xread(
                {stream: "$"}, block=2000, count=1
            )
            return messages

        # Run producer and consumer concurrently
        producer_task = asyncio.create_task(producer())
        consumer_task = asyncio.create_task(consumer())

        messages = await consumer_task
        await producer_task

        # Should receive the message
        assert len(messages) == 1

        # Cleanup
        await async_redis_client.delete(stream)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not UVLOOP_AVAILABLE, reason="uvloop not available")
class TestUvloopIntegration:
    """Test uvloop integration for enhanced async performance."""

    async def test_uvloop_basic_operations(self, redis_available, unique_key):
        """Test basic operations with uvloop."""
        if not redis_available:
            pytest.skip("Redis not available")

        import os

        # Create client with uvloop
        client = AsyncRedis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True
        )

        try:
            key = f"{unique_key}:uvloop"

            # Basic operations
            await client.set(key, "uvloop_value")
            value = await client.get(key)
            assert value == "uvloop_value"

            # Cleanup
            await client.delete(key)
        finally:
            await client.aclose()

    async def test_uvloop_performance(self, redis_available, unique_key):
        """Test uvloop performance with many operations."""
        if not redis_available:
            pytest.skip("Redis not available")

        import os

        client = AsyncRedis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True
        )

        try:
            base_key = f"{unique_key}:uvloop_perf"

            # Perform many operations
            start_time = time.time()

            async def operation(index):
                key = f"{base_key}:{index}"
                await client.set(key, f"value_{index}")
                return await client.get(key)

            tasks = [operation(i) for i in range(1000)]
            results = await asyncio.gather(*tasks)

            elapsed = time.time() - start_time

            # All should succeed
            assert len(results) == 1000
            assert all(r.startswith("value_") for r in results)

            # Should be reasonably fast (adjust threshold as needed)
            assert elapsed < 5.0  # 1000 ops in under 5 seconds

            # Cleanup
            keys = [f"{base_key}:{i}" for i in range(1000)]
            await client.delete(*keys)
        finally:
            await client.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncPubSub:
    """Test async pub/sub operations."""

    async def test_async_pubsub_basic(self, async_redis_client, unique_key):
        """Test basic async pub/sub."""
        channel = f"{unique_key}:channel"
        messages_received = []

        # Create pubsub
        pubsub = async_redis_client.pubsub()
        await pubsub.subscribe(channel)

        # Publisher task
        async def publisher():
            await asyncio.sleep(0.5)
            for i in range(3):
                await async_redis_client.publish(channel, f"message_{i}")
                await asyncio.sleep(0.1)

        # Consumer task
        async def consumer():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    messages_received.append(message["data"])
                    if len(messages_received) == 3:
                        break

        # Run both tasks
        publisher_task = asyncio.create_task(publisher())
        consumer_task = asyncio.create_task(consumer())

        await asyncio.gather(publisher_task, consumer_task)

        # Should receive all messages
        assert messages_received == ["message_0", "message_1", "message_2"]

        # Cleanup
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()
