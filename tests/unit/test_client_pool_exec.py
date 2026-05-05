import asyncio
import pytest

from cy_redis.core.cy_redis_client import CyRedisClient, CyRedisConnectionPool


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
    pool = CyRedisConnectionPool(host=host, port=port, max_connections=1)

    conn1 = pool.get_connection()
    try:
        assert conn1 is not None
        # With max_connections=1, a second checkout should return None
        conn2 = pool.get_connection()
        assert conn2 is None
    finally:
        pool.return_connection(conn1)


