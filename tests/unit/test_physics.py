"""
Unit + integration tests for CYPHYS.* (physics Redis module commands).

No-Redis tests:
    - CyPhysics import + construction
    - return-type contracts with fake Redis

Redis integration tests (require cy_game.so):
    - CYPHYS.AABB overlap / no-overlap
    - CYPHYS.SWEEP no hit / wall hit / normal direction
    - CYPHYS.CIRCLE query
    - CYPHYS.RESOLVE pushes overlapping entities apart
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
def prefix():
    return f"ptest:{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
def cleanup(redis_client, prefix):
    yield
    for k in (redis_client.execute_command(["KEYS", f"{prefix}:*"]) or []):
        redis_client.execute_command(["DEL", k])


# ---------------------------------------------------------------------------
# No-Redis: API surface
# ---------------------------------------------------------------------------

class TestCyPhysicsAPI:
    def test_import(self):
        try:
            from cyredis_game.physics import CyPhysics
        except ImportError:
            pytest.skip("CyPhysics Cython extension not built")

    def test_aabb_returns_bool(self):
        try:
            from cyredis_game.physics import CyPhysics
        except ImportError:
            pytest.skip("CyPhysics Cython extension not built")

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return 1

        ph = CyPhysics(_FakeRedis())
        result = ph.aabb(0, 0, 1, 1, 0.5, 0.5, 1, 1)
        assert isinstance(result, bool) or result in (0, 1)


# ---------------------------------------------------------------------------
# Integration: CYPHYS commands
# ---------------------------------------------------------------------------

@pytest.mark.redis
class TestCyPhysAABB:
    def test_overlap(self, redis_client, module_loaded):
        r = redis_client.execute_command([
            "CYPHYS.AABB",
            "0", "0", "2", "2",
            "1", "1", "2", "2",
        ])
        assert r == 1

    def test_no_overlap(self, redis_client, module_loaded):
        r = redis_client.execute_command([
            "CYPHYS.AABB",
            "0", "0", "1", "1",
            "10", "10", "1", "1",
        ])
        assert r == 0

    def test_edge_touching_no_overlap(self, redis_client, module_loaded):
        # Boxes touching exactly at edge — overlap is strict <
        r = redis_client.execute_command([
            "CYPHYS.AABB",
            "0", "0", "1", "1",
            "2", "0", "1", "1",
        ])
        assert r == 0


@pytest.mark.redis
class TestCyPhysSweep:
    def _make_entity(self, redis_client, prefix, eid, x, y):
        key = f"{prefix}:{eid}"
        redis_client.execute_command(["HSET", key, "x", str(x), "y", str(y)])
        return key

    def _make_obs(self, redis_client, prefix, cells):
        """cells: list of (gx, gy) that are blocked."""
        key = f"{prefix}:obs"
        for gx, gy in cells:
            redis_client.execute_command(["HSET", key, f"{gx},{gy}", "1"])
        return key

    def test_no_hit(self, redis_client, module_loaded, prefix):
        ent = self._make_entity(redis_client, prefix, "e1", 0.0, 0.0)
        obs = f"{prefix}:obs_empty"
        raw = redis_client.execute_command([
            "CYPHYS.SWEEP", ent, obs, "5.0", "0.0", "1000", "1.0",
        ])
        assert raw is not None
        assert int(raw[2]) == 0     # hit == 0
        assert abs(float(raw[0]) - 5.0) < 0.1  # new_x ≈ 5

    def test_wall_hit(self, redis_client, module_loaded, prefix):
        ent = self._make_entity(redis_client, prefix, "e2", 0.0, 0.0)
        obs = self._make_obs(redis_client, prefix, [(3, 0)])
        raw = redis_client.execute_command([
            "CYPHYS.SWEEP", ent, obs, "10.0", "0.0", "1000", "1.0",
        ])
        assert raw is not None
        assert int(raw[2]) == 1   # hit
        # Stopped before x=3
        assert float(raw[0]) < 3.5


@pytest.mark.redis
class TestCyPhysCircle:
    def test_circle_query_finds_entity(self, redis_client, module_loaded, prefix):
        spatial_key = f"{prefix}:spatial"
        # Score: floor(x*1000)*1e9 + floor(y*1000) => x=1,y=1: 1000*1e9 + 1000
        score = int(1.0 * 1000) * 1_000_000_000 + int(1.0 * 1000)
        redis_client.execute_command(["ZADD", spatial_key, str(score), "ent_A"])

        raw = redis_client.execute_command([
            "CYPHYS.CIRCLE", spatial_key, "1.0", "1.0", "5.0", "10",
        ])
        assert raw is not None
        ids = [raw[i] for i in range(0, len(raw) - 1, 2)]
        assert "ent_A" in ids

    def test_circle_query_excludes_far_entity(self, redis_client, module_loaded, prefix):
        spatial_key = f"{prefix}:spatial2"
        score = int(100.0 * 1000) * 1_000_000_000 + int(100.0 * 1000)
        redis_client.execute_command(["ZADD", spatial_key, str(score), "far_ent"])

        raw = redis_client.execute_command([
            "CYPHYS.CIRCLE", spatial_key, "0.0", "0.0", "5.0", "10",
        ])
        ids = [raw[i] for i in range(0, len(raw) - 1, 2)] if raw else []
        assert "far_ent" not in ids


@pytest.mark.redis
class TestCyPhysResolve:
    def _make_entity(self, redis_client, prefix, eid, x, y, w=0.5, h=0.5):
        key = f"{prefix}:{eid}"
        redis_client.execute_command([
            "HSET", key,
            "x", str(x), "y", str(y), "w", str(w), "h", str(h),
        ])
        return key

    def test_resolve_pushes_apart(self, redis_client, module_loaded, prefix):
        a = self._make_entity(redis_client, prefix, "a", 0.0, 0.0)
        b = self._make_entity(redis_client, prefix, "b", 0.5, 0.0)
        raw = redis_client.execute_command(["CYPHYS.RESOLVE", a, b])
        assert raw is not None and len(raw) >= 2
        dx, dy = float(raw[0]), float(raw[1])
        # Some separation must have been applied
        assert abs(dx) + abs(dy) > 0

    def test_resolve_no_overlap_returns_zero(self, redis_client, module_loaded, prefix):
        a = self._make_entity(redis_client, prefix, "a2", 0.0, 0.0)
        b = self._make_entity(redis_client, prefix, "b2", 10.0, 0.0)
        raw = redis_client.execute_command(["CYPHYS.RESOLVE", a, b])
        dx, dy = float(raw[0]), float(raw[1])
        assert dx == 0.0 and dy == 0.0
