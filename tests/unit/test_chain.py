"""
Unit + integration tests for CyFunctionChain.

No-Redis tests:
    - Command assembly (correct FCALL format)
    - add_command builds correct list
    - reset clears pending steps
    - execute with fake pipeline

Redis integration tests (require Lua functions loaded):
    - Two-step chain: FCALL + ZADD
    - Error in one step doesn't abort pipeline
    - Result list has correct length
"""

import uuid

import pytest

from cy_redis.core.cy_redis_client import CyRedisClient
from cyredis_game.game_engine import GameEngine

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def redis_client():
    try:
        c = CyRedisClient(host="localhost", port=6379)
        c.set("_probe", "1")
        return c
    except Exception:
        pytest.skip("Redis not available")


@pytest.fixture(scope="session")
def game_engine(redis_client):
    eng = GameEngine(redis_client)
    eng.load_functions()
    return eng


@pytest.fixture
def prefix():
    return f"chain_test:{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
def cleanup(redis_client, prefix):
    yield
    for k in redis_client.execute_command(["KEYS", f"{prefix}:*"]) or []:
        redis_client.execute_command(["DEL", k])


# ---------------------------------------------------------------------------
# No-Redis: API surface + protocol assembly
# ---------------------------------------------------------------------------


class TestCyFunctionChainAPI:
    def test_import(self):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

    def test_add_fcall_builds_fcall_command(self):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        captured = []

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                captured.append(list(a) if len(a) > 1 else a[0])
                return None

        chain = CyFunctionChain(_FakeRedis(), None)
        chain.add_fcall("my_fn", ["key1", "key2"], ["arg1"])
        assert chain._steps == [["FCALL", "my_fn", "2", "key1", "key2", "arg1"]]

    def test_add_command_stored(self):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return None

        chain = CyFunctionChain(_FakeRedis(), None)
        chain.add_command(["SET", "mykey", "val"])
        assert chain._steps == [["SET", "mykey", "val"]]

    def test_reset_clears_steps(self):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return None

        chain = CyFunctionChain(_FakeRedis(), None)
        chain.add_command(["SET", "k", "v"])
        chain.reset()
        assert chain._steps == []

    def test_execute_returns_list(self):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        calls = []

        class _FakeRedis:
            def execute_command(self, cmd):
                calls.append(cmd)
                return "OK"

        chain = CyFunctionChain(_FakeRedis(), None)
        chain.add_command(["SET", "k", "v"])
        results = chain.execute()
        assert isinstance(results, list)
        assert len(results) == 1

    def test_execute_resets_after_call(self):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return "OK"

        chain = CyFunctionChain(_FakeRedis(), None)
        chain.add_command(["SET", "k", "v"])
        chain.execute()
        assert chain._steps == []


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------


@pytest.mark.redis
class TestCyFunctionChainIntegration:
    def test_two_step_chain_set_and_zadd(self, redis_client, game_engine, prefix):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        set_key = f"{prefix}:kv"
        zset_key = f"{prefix}:zset"
        member = f"m_{uuid.uuid4().hex[:6]}"

        chain = CyFunctionChain(redis_client, game_engine._engine.func_mgr)
        chain.add_command(["SET", set_key, "hello"])
        chain.add_command(["ZADD", zset_key, "1.5", member])
        results = chain.execute()

        assert len(results) == 2
        assert redis_client.execute_command(["GET", set_key]) == "hello"
        score = redis_client.execute_command(["ZSCORE", zset_key, member])
        assert score is not None
        assert abs(float(score) - 1.5) < 0.001

    def test_empty_chain_returns_empty_list(self, redis_client, game_engine, prefix):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        chain = CyFunctionChain(redis_client, game_engine._engine.func_mgr)
        results = chain.execute()
        assert results == []

    def test_multiple_executions_independent(self, redis_client, game_engine, prefix):
        try:
            from cyredis_game.chain import CyFunctionChain
        except ImportError:
            pytest.skip("CyFunctionChain Cython extension not built")

        k1 = f"{prefix}:k1"
        k2 = f"{prefix}:k2"

        chain = CyFunctionChain(redis_client, game_engine._engine.func_mgr)
        chain.add_command(["SET", k1, "first"])
        chain.execute()

        chain.add_command(["SET", k2, "second"])
        chain.execute()

        assert redis_client.execute_command(["GET", k1]) == "first"
        assert redis_client.execute_command(["GET", k2]) == "second"
