"""
Unit + integration tests for the FLECS Redis module (cy_game.so).

No-Redis tests:
    - CyGameModule construction
    - GameEngine.load_module / init_world / restore_world API surface

Redis integration tests (require cy_game.so to be built and loaded):
    - FLECS.INIT, FLECS.SPAWN, FLECS.GETCOMP, FLECS.SETCOMP, FLECS.HAS
    - FLECS.TICK advances position via velocity
    - FLECS.QUERY returns matching entity IDs
    - FLECS.FINI + FLECS.RESTORE round-trip
    - Damage via FLECS.SETCOMP
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
    """Load cy_game.so (skip if not built)."""
    if not os.path.exists(_SO_PATH):
        pytest.skip("cy_game.so not built — run: make module")
    try:
        redis_client.execute_command(["MODULE", "LOAD", _SO_PATH])
    except Exception as e:
        if "already" not in str(e).lower():
            pytest.skip(f"Could not load cy_game.so: {e}")


@pytest.fixture
def world_id():
    wid = f"tw_{uuid.uuid4().hex[:8]}"
    yield wid


@pytest.fixture
def zone_id():
    return f"tz_{uuid.uuid4().hex[:6]}"


@pytest.fixture
def init_world(redis_client, module_loaded, world_id):
    """Create + yield a fresh FLECS world; teardown with FLECS.FINI."""
    redis_client.execute_command(["FLECS.INIT", world_id])
    yield world_id
    try:
        redis_client.execute_command(["FLECS.FINI", world_id])
    except Exception:
        pass


# ---------------------------------------------------------------------------
# No-Redis: API surface
# ---------------------------------------------------------------------------


class TestCyGameModuleAPI:
    """CyGameModule exposes correct Python API without a Redis connection."""

    def test_module_manager_import(self):
        # If Cython extension not yet built, this is a soft skip.
        try:
            from cyredis_game.module_manager import CyGameModule
        except ImportError:
            pytest.skip("CyGameModule Cython extension not built")

    def test_game_engine_has_load_module(self):
        from cyredis_game.game_engine import GameEngine

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return None

        eng = GameEngine(_FakeRedis())
        assert hasattr(eng, "load_module")
        assert hasattr(eng, "init_world")
        assert hasattr(eng, "restore_world")


# ---------------------------------------------------------------------------
# Integration: FLECS world lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.redis
class TestFlecsWorldLifecycle:
    def test_init_world(self, redis_client, module_loaded, world_id):
        result = redis_client.execute_command(["FLECS.INIT", world_id])
        assert result == 1
        # Duplicate init returns error
        with pytest.raises(Exception, match="already"):
            redis_client.execute_command(["FLECS.INIT", world_id])
        redis_client.execute_command(["FLECS.FINI", world_id])

    def test_fini_world(self, redis_client, init_world):
        result = redis_client.execute_command(["FLECS.FINI", init_world])
        assert result == 1

    def test_stats(self, redis_client, init_world):
        raw = redis_client.execute_command(["FLECS.STATS", init_world])
        assert raw is not None
        stats = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        assert "world" in stats
        assert stats["world"] == init_world


@pytest.mark.redis
class TestFlecsEntityLifecycle:
    def test_spawn_entity(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        r = redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "player",
                "10.0",
                "20.0",
                "1.0",
                "0.5",
                "100",
            ]
        )
        assert r == 1

    def test_spawn_duplicate_rejected(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "player",
                "0",
                "0",
            ]
        )
        r2 = redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "player",
                "5",
                "5",
            ]
        )
        assert r2 == 0

    def test_getcomp_position(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "npc",
                "3.5",
                "7.25",
            ]
        )
        raw = redis_client.execute_command(
            ["FLECS.GETCOMP", init_world, eid, "Position"]
        )
        comp = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        assert abs(float(comp["x"]) - 3.5) < 0.001
        assert abs(float(comp["y"]) - 7.25) < 0.001

    def test_getcomp_health(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "npc",
                "0",
                "0",
                "0",
                "0",
                "80",
            ]
        )
        raw = redis_client.execute_command(["FLECS.GETCOMP", init_world, eid, "Health"])
        comp = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        assert int(comp["hp"]) == 80

    def test_setcomp_position(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "npc",
                "0",
                "0",
            ]
        )
        redis_client.execute_command(
            [
                "FLECS.SETCOMP",
                init_world,
                eid,
                "Position",
                "x",
                "42.0",
                "y",
                "99.5",
            ]
        )
        raw = redis_client.execute_command(
            ["FLECS.GETCOMP", init_world, eid, "Position"]
        )
        comp = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        assert abs(float(comp["x"]) - 42.0) < 0.001
        assert abs(float(comp["y"]) - 99.5) < 0.001

    def test_has_component(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "npc",
                "0",
                "0",
            ]
        )
        assert (
            redis_client.execute_command(["FLECS.HAS", init_world, eid, "Position"])
            == 1
        )
        assert (
            redis_client.execute_command(["FLECS.HAS", init_world, eid, "Health"]) == 1
        )
        assert (
            redis_client.execute_command(
                ["FLECS.HAS", init_world, "nonexistent_eid", "Position"]
            )
            == 0
        )

    def test_delete_entity(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "npc",
                "0",
                "0",
            ]
        )
        r = redis_client.execute_command(["FLECS.DELETE", init_world, eid])
        assert r == 1
        # Deleted entity no longer exists
        assert (
            redis_client.execute_command(["FLECS.HAS", init_world, eid, "Position"])
            == 0
        )


@pytest.mark.redis
class TestFlecsTickMovement:
    def test_tick_advances_counter(self, redis_client, init_world, zone_id):
        raw = redis_client.execute_command(["FLECS.TICK", init_world, zone_id, "100"])
        assert raw is not None
        tick_map = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        assert int(tick_map["tick"]) >= 1

    def test_tick_moves_entities(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "player",
                "0.0",
                "0.0",  # start at origin
                "10.0",
                "0.0",  # vx=10, vy=0
                "100",
            ]
        )
        # Tick 1 second (1000 ms) → x should advance by 10 units
        redis_client.execute_command(["FLECS.TICK", init_world, zone_id, "1000"])
        raw = redis_client.execute_command(
            ["FLECS.GETCOMP", init_world, eid, "Position"]
        )
        comp = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
        assert abs(float(comp["x"]) - 10.0) < 0.1
        assert abs(float(comp["y"])) < 0.01


@pytest.mark.redis
class TestFlecsQuery:
    def test_query_returns_spawned_entity(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "player",
                "0",
                "0",
            ]
        )
        results = redis_client.execute_command(
            [
                "FLECS.QUERY",
                init_world,
                "Position, Health",
                zone_id,
            ]
        )
        assert eid in [str(r) for r in (results or [])]

    def test_query_no_zone_filter(self, redis_client, init_world, zone_id):
        eid = f"p_{uuid.uuid4().hex[:6]}"
        redis_client.execute_command(
            [
                "FLECS.SPAWN",
                init_world,
                zone_id,
                eid,
                "npc",
                "5",
                "5",
            ]
        )
        results = redis_client.execute_command(
            [
                "FLECS.QUERY",
                init_world,
                "Position",
            ]
        )
        assert eid in [str(r) for r in (results or [])]


@pytest.mark.redis
class TestFlecsRestore:
    def test_restore_world_reloads_entities(self, redis_client, module_loaded):
        world_id = f"rw_{uuid.uuid4().hex[:8]}"
        zone_id = f"rz_{uuid.uuid4().hex[:6]}"
        eid = f"p_{uuid.uuid4().hex[:6]}"

        try:
            redis_client.execute_command(["FLECS.INIT", world_id])
            redis_client.execute_command(
                [
                    "FLECS.SPAWN",
                    world_id,
                    zone_id,
                    eid,
                    "hero",
                    "5.0",
                    "5.0",
                ]
            )
            # Destroy the in-memory world
            redis_client.execute_command(["FLECS.FINI", world_id])
            # Verify entity is gone from FLECS
            assert (
                redis_client.execute_command(["FLECS.HAS", world_id, eid, "Position"])
                == 0
            )

            # Restore from Redis persisted data
            n = redis_client.execute_command(["FLECS.RESTORE", world_id])
            assert int(n) >= 1

            # Entity should be back
            assert (
                redis_client.execute_command(["FLECS.HAS", world_id, eid, "Position"])
                == 1
            )

            raw = redis_client.execute_command(
                ["FLECS.GETCOMP", world_id, eid, "Position"]
            )
            comp = {raw[i]: raw[i + 1] for i in range(0, len(raw) - 1, 2)}
            assert abs(float(comp["x"]) - 5.0) < 0.01
        finally:
            try:
                redis_client.execute_command(["FLECS.FINI", world_id])
            except Exception:
                pass
            # Clean up Redis persistence
            for key in (
                redis_client.execute_command(
                    ["KEYS", f"cy:ent:{{{world_id}:{zone_id}}}:*"]
                )
                or []
            ):
                redis_client.execute_command(["DEL", key])
