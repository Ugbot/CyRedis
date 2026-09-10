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

import uuid

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def redis_client(module_loaded):
    """These tests talk to whichever server holds the cy_game module."""
    return module_loaded


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
        raw = redis_client.execute_command(
            [
                "CYPATH.FIND",
                grid_key,
                "0",
                "0",
                "4",
                "0",
            ]
        )
        assert raw is not None
        # Expect pairs [x,y, ...] ending at (4,0)
        assert len(raw) >= 2
        assert int(raw[-2]) == 4
        assert int(raw[-1]) == 0

    def test_path_around_wall(self, redis_client, module_loaded, grid_key):
        # The grid is an unbounded plane, so a wall only diverts the route:
        # the path must simply never step on a blocked cell.
        blocked = {(2, y) for y in range(-4, 5)}
        for x, y in blocked:
            redis_client.execute_command(["CYPATH.SET", grid_key, str(x), str(y), "1"])
        raw = redis_client.execute_command(
            [
                "CYPATH.FIND",
                grid_key,
                "0",
                "2",
                "4",
                "2",
            ]
        )
        assert raw
        waypoints = [(int(raw[i]), int(raw[i + 1])) for i in range(0, len(raw) - 1, 2)]
        assert waypoints[-1] == (4, 2)
        assert not blocked.intersection(waypoints)

    def test_no_route(self, redis_client, module_loaded, grid_key):
        # Movement is 4-directional, so sealing the goal's four neighbours
        # makes it unreachable even on an otherwise open plane.
        for x, y in ((4, 0), (6, 0), (5, 1), (5, -1)):
            redis_client.execute_command(["CYPATH.SET", grid_key, str(x), str(y), "1"])
        raw = redis_client.execute_command(
            [
                "CYPATH.FIND",
                grid_key,
                "0",
                "0",
                "5",
                "0",
            ]
        )
        assert raw == [] or raw is None

    def test_start_equals_goal(self, redis_client, module_loaded, grid_key):
        raw = redis_client.execute_command(
            [
                "CYPATH.FIND",
                grid_key,
                "5",
                "5",
                "5",
                "5",
            ]
        )
        assert raw == [] or raw is None

    def test_max_steps_limits_search(self, redis_client, module_loaded, grid_key):
        # 1 max_step: impossible to reach (100,100) from (0,0) in 1 step
        raw = redis_client.execute_command(
            [
                "CYPATH.FIND",
                grid_key,
                "0",
                "0",
                "100",
                "100",
                "1",
            ]
        )
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
