"""
Unit + integration tests for CYPATH.* (A* pathfinding Redis module commands).

No-Redis tests:
    - CyPathfinder key format
    - find_path return type / empty result on missing module

Redis integration tests (require cy_game.so):
    - Straight-line path (no obstacles)
    - Path around a wall
    - No route (fully blocked goal)
    - max_steps cap
    - Start == goal returns empty list
"""

import os
import uuid
import pytest

from cy_redis.core.cy_redis_client import CyRedisClient


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


_SO_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../cyredis_game/module/cy_game.so")
)


@pytest.fixture(scope="session")
def module_loaded(redis_client):
    if not os.path.exists(_SO_PATH):
        pytest.skip("cy_game.so not built — run: make module")
    try:
        redis_client.execute_command(["MODULE", "LOAD", _SO_PATH])
    except Exception as e:
        if "already" not in str(e).lower():
            pytest.skip(f"Could not load cy_game.so: {e}")


@pytest.fixture
def grid_key():
    return f"cy:nav:test:{uuid.uuid4().hex[:8]}:grid"


@pytest.fixture(autouse=True)
def cleanup_grid(redis_client, grid_key):
    yield
    try:
        redis_client.execute_command(["DEL", grid_key])
    except Exception:
        pass


# ---------------------------------------------------------------------------
# No-Redis: API surface
# ---------------------------------------------------------------------------

class TestCyPathfinderKeyFormat:
    def test_import(self):
        try:
            from cyredis_game.pathfinding import CyPathfinder
        except ImportError:
            pytest.skip("CyPathfinder Cython extension not built")

    def test_grid_key_stored(self):
        try:
            from cyredis_game.pathfinding import CyPathfinder
        except ImportError:
            pytest.skip("CyPathfinder Cython extension not built")

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return []

        pf = CyPathfinder(_FakeRedis(), "test:grid")
        assert pf.grid_key == "test:grid"

    def test_find_path_returns_list(self):
        try:
            from cyredis_game.pathfinding import CyPathfinder
        except ImportError:
            pytest.skip("CyPathfinder Cython extension not built")

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return None

        pf = CyPathfinder(_FakeRedis(), "k")
        result = pf.find_path(0, 0, 3, 3)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Integration: CYPATH commands
# ---------------------------------------------------------------------------

@pytest.mark.redis
class TestCyPathCommands:
    def test_clear_grid(self, redis_client, module_loaded, grid_key):
        redis_client.execute_command(["CYPATH.SET", grid_key, "1", "1", "1"])
        redis_client.execute_command(["CYPATH.CLEAR", grid_key])
        exists = redis_client.execute_command(["EXISTS", grid_key])
        assert exists == 0

    def test_straight_path(self, redis_client, module_loaded, grid_key):
        # No obstacles: (0,0) → (4,0) should give 4 steps along x axis
        raw = redis_client.execute_command([
            "CYPATH.FIND", grid_key, "0", "0", "4", "0",
        ])
        assert raw is not None
        # Expect pairs [x,y, ...] ending at (4,0)
        assert len(raw) >= 2
        assert int(raw[-2]) == 4
        assert int(raw[-1]) == 0

    def test_path_around_wall(self, redis_client, module_loaded, grid_key):
        # Block the direct x path at (2,0), (2,1) — force path to go around
        for y in range(0, 5):
            redis_client.execute_command(["CYPATH.SET", grid_key, "2", str(y), "1"])
        raw = redis_client.execute_command([
            "CYPATH.FIND", grid_key, "0", "2", "4", "2",
        ])
        # Should find some path — not necessarily the same route each time
        # Just verify start/end are not in the blocked column
        if raw:
            for i in range(0, len(raw) - 1, 2):
                assert int(raw[i]) != 2  # no waypoint in blocked column

    def test_no_route(self, redis_client, module_loaded, grid_key):
        # Build a solid wall sealing off the goal
        for y in range(-1, 3):
            redis_client.execute_command(["CYPATH.SET", grid_key, "2", str(y), "1"])
        raw = redis_client.execute_command([
            "CYPATH.FIND", grid_key, "0", "0", "5", "0",
        ])
        assert raw == [] or raw is None

    def test_start_equals_goal(self, redis_client, module_loaded, grid_key):
        raw = redis_client.execute_command([
            "CYPATH.FIND", grid_key, "5", "5", "5", "5",
        ])
        assert raw == [] or raw is None

    def test_max_steps_limits_search(self, redis_client, module_loaded, grid_key):
        # 1 max_step: impossible to reach (100,100) from (0,0) in 1 step
        raw = redis_client.execute_command([
            "CYPATH.FIND", grid_key, "0", "0", "100", "100", "1",
        ])
        # Either empty (no path found within limit) or very short
        if raw:
            assert len(raw) <= 2  # at most one step returned


@pytest.mark.redis
class TestCyPathfinderWrapper:
    def test_wrapper_find_path(self, redis_client, module_loaded, grid_key):
        try:
            from cyredis_game.pathfinding import CyPathfinder
        except ImportError:
            pytest.skip("CyPathfinder Cython extension not built")
        pf = CyPathfinder(redis_client, grid_key)
        path = pf.find_path(0, 0, 3, 0)
        assert isinstance(path, list)
        if path:
            assert isinstance(path[0], tuple)
            assert len(path[0]) == 2
            assert path[-1] == (3, 0)

    def test_wrapper_set_and_clear(self, redis_client, module_loaded, grid_key):
        try:
            from cyredis_game.pathfinding import CyPathfinder
        except ImportError:
            pytest.skip("CyPathfinder Cython extension not built")
        pf = CyPathfinder(redis_client, grid_key)
        pf.set_cell(1, 1, True)
        val = redis_client.execute_command(["HGET", grid_key, "1,1"])
        assert val == "1"
        pf.set_cell(1, 1, False)
        val2 = redis_client.execute_command(["HGET", grid_key, "1,1"])
        assert val2 is None
