import asyncio
import pytest

from cy_redis.core.cy_redis_client import (
    CyRedisClient,
    CyRedisConnectionPool,
    ConnectionError as CyConnectionError,
)


@pytest.mark.redis
def test_execute_async_delegates_and_returns():
    host, port = "localhost", 6379
    key = "test:execute_async:key"
    value = "v1"

    async def run():
        client = CyRedisClient(host=host, port=port, max_connections=2)
        await client.execute_async("set", key, value)
        result = await client.execute_async("get", key)
        await client.execute_async("delete", key)
        return result

    result = asyncio.run(run())
    assert result == value


@pytest.mark.redis
def test_pool_respects_max_connections():
    host, port = "localhost", 6379
    # Short wait_timeout so the exhaustion path returns promptly.
    pool = CyRedisConnectionPool(
        host=host, port=port, max_connections=1, wait_timeout=0.25
    )

    conn1 = pool.get_connection()
    try:
        assert conn1 is not None
        # With max_connections=1 and the only connection checked out, a second
        # checkout must raise ConnectionError (never hand back None for callers
        # to dereference).
        with pytest.raises(CyConnectionError):
            pool.get_connection()
    finally:
        pool.return_connection(conn1)


@pytest.mark.redis
def test_pool_recovers_after_return():
    """A slot freed by return_connection is reusable (semaphore not leaked)."""
    host, port = "localhost", 6379
    pool = CyRedisConnectionPool(
        host=host, port=port, max_connections=1, wait_timeout=0.25
    )
    conn1 = pool.get_connection()
    pool.return_connection(conn1)
    # Slot was released; we can check out again without hitting the timeout.
    conn2 = pool.get_connection()
    assert conn2 is not None
    pool.return_connection(conn2)


