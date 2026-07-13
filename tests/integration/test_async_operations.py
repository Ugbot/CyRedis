"""
Integration tests for the native async API (`*_async` coroutines).

Every operation goes through CyRedisClient's own async surface — the
executor-backed coroutines over the native pool — plus the async stream and
pub/sub iterators. No redis-py anywhere: this suite exercises the library
being shipped, including event-loop integration and (when installed) uvloop.
"""

import asyncio
import random
import string
import time

import pytest

from cy_redis.utils.redis_iterators import RedisPubSubIterator

try:
    import uvloop

    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False


def _random_value(k: int = 24) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=k))


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncBasicOperations:
    """Basic operations through the native async coroutines."""

    async def test_async_set_get(self, redis_client, unique_key):
        key = f"{unique_key}:async"
        value = _random_value()

        assert await redis_client.set_async(key, value) is True
        assert await redis_client.get_async(key) == value
        await redis_client.delete_async(key)

    async def test_async_mset_mget(self, redis_client, unique_key):
        keys = [f"{unique_key}:async:{i}" for i in range(10)]
        values = [_random_value() for _ in range(10)]

        # execute_async dispatches to the client method of that name.
        mapping = dict(zip(keys, values))
        assert await redis_client.execute_async("mset", mapping) is True

        retrieved = await redis_client.execute_async("mget", *keys)
        assert retrieved == values

        await redis_client.delete_async(*keys)

    async def test_async_incr(self, redis_client, unique_key):
        key = f"{unique_key}:counter"

        assert await redis_client.execute_async("incr", key) == 1
        assert await redis_client.execute_async("incr", key) == 2

        await redis_client.delete_async(key)

    async def test_async_expire(self, redis_client, unique_key):
        key = f"{unique_key}:expire"

        await redis_client.set_async(key, _random_value())
        assert await redis_client.execute_async("expire", key, 1) == 1
        assert await redis_client.execute_async("exists", key) == 1

        await asyncio.sleep(1.2)
        assert await redis_client.execute_async("exists", key) == 0


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncPipeline:
    """The native pipeline driven from an async context.

    The pipeline itself is synchronous by design (one buffered round trip);
    from a coroutine it belongs on a worker thread via asyncio.to_thread so
    the event loop never blocks.
    """

    async def test_pipeline_from_async_context(self, redis_client, unique_key):
        keys = [f"{unique_key}:pipe:{i}" for i in range(5)]
        values = [_random_value() for _ in range(5)]

        def run_pipeline():
            pipe = redis_client.pipeline()
            for key, value in zip(keys, values):
                pipe.set(key, value)
            return pipe.execute()

        results = await asyncio.to_thread(run_pipeline)
        assert all(r in (True, "OK") for r in results)

        retrieved = await asyncio.gather(*(redis_client.get_async(k) for k in keys))
        assert retrieved == values

        await redis_client.delete_async(*keys)

    async def test_pipeline_mixed_operations(self, redis_client, unique_key):
        key1 = f"{unique_key}:pipe1"
        key2 = f"{unique_key}:pipe2"
        start = random.randint(1, 10**6)
        value2 = _random_value()

        await redis_client.set_async(key1, str(start))

        def run_pipeline():
            pipe = redis_client.pipeline()
            pipe.incr(key1)
            pipe.get(key1)
            pipe.set(key2, value2)
            pipe.get(key2)
            return pipe.execute()

        results = await asyncio.to_thread(run_pipeline)
        assert results[0] == start + 1
        assert results[1] == str(start + 1)
        assert results[2] in (True, "OK")
        assert results[3] == value2

        await redis_client.delete_async(key1, key2)


@pytest.mark.integration
@pytest.mark.asyncio
class TestConcurrentAsyncOperations:
    """Concurrency through the executor-backed coroutines."""

    async def test_concurrent_async_reads(self, redis_client, unique_key):
        key = f"{unique_key}:concurrent"
        value = _random_value()
        await redis_client.set_async(key, value)

        results = await asyncio.gather(
            *(redis_client.get_async(key) for _ in range(100))
        )
        assert len(results) == 100
        assert all(r == value for r in results)

        await redis_client.delete_async(key)

    async def test_concurrent_async_writes(self, redis_client, unique_key):
        base_key = f"{unique_key}:concurrent"
        expected = {f"{base_key}:{i}": _random_value() for i in range(100)}

        await asyncio.gather(
            *(redis_client.set_async(k, v) for k, v in expected.items())
        )

        retrieved = await asyncio.gather(*(redis_client.get_async(k) for k in expected))
        assert retrieved == list(expected.values())

        await redis_client.delete_async(*expected.keys())

    async def test_concurrent_async_incr_is_atomic(self, redis_client, unique_key):
        key = f"{unique_key}:atomic_counter"
        n = random.randint(50, 150)

        results = await asyncio.gather(
            *(redis_client.execute_async("incr", key) for _ in range(n))
        )
        # Every increment observed a distinct value and the total is exact.
        assert sorted(results) == list(range(1, n + 1))
        assert int(await redis_client.get_async(key)) == n

        await redis_client.delete_async(key)

    async def test_many_concurrent_operations(self, redis_client, unique_key):
        base_key = f"{unique_key}:many"
        counter_key = f"{base_key}:counter"

        async def mixed_operation(index):
            key = f"{base_key}:{index}"
            value = f"value_{index}"
            await redis_client.set_async(key, value)
            retrieved = await redis_client.get_async(key)
            await redis_client.execute_async("incr", counter_key)
            return retrieved == value

        results = await asyncio.gather(*(mixed_operation(i) for i in range(500)))
        assert all(results)
        assert int(await redis_client.get_async(counter_key)) == 500

        keys = [f"{base_key}:{i}" for i in range(500)] + [counter_key]
        await redis_client.delete_async(*keys)


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncTransactions:
    """WATCH/MULTI optimistic locking driven from an async context."""

    async def test_watch_multi_exec(self, redis_client, unique_key):
        key = f"{unique_key}:watch"
        start = random.randint(0, 10**6)
        await redis_client.set_async(key, str(start))

        def transaction():
            pipe = redis_client.pipeline()
            pipe.watch(key)
            current = int(redis_client.get(key))
            pipe.multi()
            pipe.set(key, str(current + 1))
            return pipe.execute()

        results = await asyncio.to_thread(transaction)
        assert results  # EXEC succeeded (no watched-key conflict)
        assert await redis_client.get_async(key) == str(start + 1)

        await redis_client.delete_async(key)


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncStreamOperations:
    """Native async stream coroutines."""

    async def test_async_xadd_xread(self, redis_client, unique_key):
        stream = f"{unique_key}:stream"
        payloads = [{f"field{i}": _random_value(8)} for i in range(2)]

        id1 = await redis_client.xadd_async(stream, payloads[0])
        id2 = await redis_client.xadd_async(stream, payloads[1])
        assert id1 and id2 and id1 != id2

        messages = await redis_client.xread_async({stream: "0"})
        assert len(messages) == 2

        await redis_client.delete_async(stream)

    async def test_async_xread_blocking(self, redis_client, unique_key):
        stream = f"{unique_key}:stream"
        payload = {"data": _random_value()}

        async def producer():
            await asyncio.sleep(0.5)
            await redis_client.xadd_async(stream, payload)

        async def consumer():
            return await redis_client.xread_async({stream: "$"}, count=1, block=3000)

        producer_task = asyncio.create_task(producer())
        messages = await consumer()
        await producer_task

        assert len(messages) == 1

        await redis_client.delete_async(stream)


@pytest.mark.integration
@pytest.mark.asyncio
class TestAsyncPubSub:
    """The native async pub/sub iterator."""

    async def test_pubsub_receives_published_messages(self, redis_client, unique_key):
        channel = f"{unique_key}:channel"
        expected = [f"message_{i}_{_random_value(6)}" for i in range(3)]
        received = []

        iterator = RedisPubSubIterator(redis_client, [channel], timeout_ms=5000)
        try:

            async def publisher():
                # Give the subscriber time to be registered server-side.
                await asyncio.sleep(0.5)
                for message in expected:
                    await asyncio.to_thread(redis_client.publish, channel, message)
                    await asyncio.sleep(0.05)

            async def consumer():
                async for message in iterator:
                    if message and message["type"] == "message":
                        received.append(message["data"])
                        if len(received) == len(expected):
                            break

            await asyncio.gather(publisher(), consumer())
            assert received == expected
        finally:
            iterator.close()


@pytest.mark.integration
@pytest.mark.skipif(not UVLOOP_AVAILABLE, reason="uvloop not available")
class TestUvloopIntegration:
    """The same native coroutines on a uvloop event loop.

    Deliberately sync test methods: each builds its own uvloop loop, because
    swapping the policy under pytest-asyncio's running loop is not possible.
    """

    def _run(self, coro):
        loop = uvloop.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_uvloop_basic_operations(self, redis_client, unique_key):
        key = f"{unique_key}:uvloop"
        value = _random_value()

        async def scenario():
            await redis_client.set_async(key, value)
            retrieved = await redis_client.get_async(key)
            await redis_client.delete_async(key)
            return retrieved

        assert self._run(scenario()) == value

    def test_uvloop_many_operations(self, redis_client, unique_key):
        base_key = f"{unique_key}:uvloop_perf"
        n = 500

        async def scenario():
            async def op(i):
                key = f"{base_key}:{i}"
                await redis_client.set_async(key, f"value_{i}")
                return await redis_client.get_async(key)

            results = await asyncio.gather(*(op(i) for i in range(n)))
            await redis_client.delete_async(*(f"{base_key}:{i}" for i in range(n)))
            return results

        start = time.monotonic()
        results = self._run(scenario())
        elapsed = time.monotonic() - start

        assert len(results) == n
        assert all(r == f"value_{i}" for i, r in enumerate(results))
        # Sanity bound, not a benchmark: n round trips shouldn't take >10s
        # against a local/CI-service Redis.
        assert elapsed < 10.0, f"{n} ops took {elapsed:.1f}s"
