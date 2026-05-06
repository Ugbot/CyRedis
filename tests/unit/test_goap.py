"""
Unit + integration tests for CYGOAP.* (GOAP planner Redis module commands).

No-Redis tests:
    - CyGOAP import + API surface
    - define_action JSON serialisation

Redis integration tests (require cy_game.so):
    - Set state, define action, plan succeeds
    - Plan returns empty list when no solution
    - plan respects max_depth
    - CYGOAP.APPLY succeeds / fails precondition check
    - CyGOAP wrapper integration
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
def ws_key():
    return f"cy:ws:test:{uuid.uuid4().hex[:8]}"


@pytest.fixture
def actions_key():
    return f"cy:actions:test:{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
def cleanup(redis_client, ws_key, actions_key):
    yield
    for k in (ws_key, actions_key):
        try:
            redis_client.execute_command(["DEL", k])
        except Exception:
            pass


# ---------------------------------------------------------------------------
# No-Redis: API surface
# ---------------------------------------------------------------------------

class TestCyGOAPAPI:
    def test_import(self):
        try:
            from cyredis_game.goap import CyGOAP
        except ImportError:
            pytest.skip("CyGOAP Cython extension not built")

    def test_construction(self):
        try:
            from cyredis_game.goap import CyGOAP
        except ImportError:
            pytest.skip("CyGOAP Cython extension not built")

        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return None

        g = CyGOAP(_FakeRedis(), "ws_key", "act_key")
        assert g.ws_key == "ws_key"
        assert g.actions_key == "act_key"


# ---------------------------------------------------------------------------
# Integration: CYGOAP commands
# ---------------------------------------------------------------------------

@pytest.mark.redis
class TestCyGOAPSetState:
    def test_setstate_single(self, redis_client, module_loaded, ws_key, actions_key):
        redis_client.execute_command([
            "CYGOAP.SETSTATE", ws_key, "has_sword", "1",
        ])
        val = redis_client.execute_command(["HGET", ws_key, "has_sword"])
        assert val == "1"

    def test_setstate_multi(self, redis_client, module_loaded, ws_key, actions_key):
        redis_client.execute_command([
            "CYGOAP.SETSTATE", ws_key,
            "has_key", "1", "door_open", "0", "enemy_dead", "0",
        ])
        assert redis_client.execute_command(["HGET", ws_key, "has_key"])    == "1"
        assert redis_client.execute_command(["HGET", ws_key, "door_open"])  == "0"
        assert redis_client.execute_command(["HGET", ws_key, "enemy_dead"]) == "0"


@pytest.mark.redis
class TestCyGOAPDefAction:
    def test_defaction_stored(self, redis_client, module_loaded, ws_key, actions_key):
        redis_client.execute_command([
            "CYGOAP.DEFACTION", actions_key, "pickup_key",
            '{"near_key": true}',  # pre
            '{"has_key": true}',   # eff
            "1",                   # cost
        ])
        raw = redis_client.execute_command(["HGET", actions_key, "pickup_key"])
        assert raw is not None
        import json
        data = json.loads(raw)
        assert "pre" in data
        assert "eff" in data


@pytest.mark.redis
class TestCyGOAPPlan:
    def _setup_scenario(self, redis_client, ws_key, actions_key):
        """Classic 'unlock door' GOAP scenario."""
        # Initial state: key not held, door closed
        redis_client.execute_command([
            "CYGOAP.SETSTATE", ws_key,
            "near_key", "1",
            "has_key",  "0",
            "door_open", "0",
        ])
        # Action 1: pickup key (pre: near_key=true, eff: has_key=true)
        redis_client.execute_command([
            "CYGOAP.DEFACTION", actions_key, "pickup_key",
            '{"near_key": true}', '{"has_key": true}', "1",
        ])
        # Action 2: open door (pre: has_key=true, eff: door_open=true)
        redis_client.execute_command([
            "CYGOAP.DEFACTION", actions_key, "open_door",
            '{"has_key": true}', '{"door_open": true}', "1",
        ])

    def test_plan_finds_sequence(self, redis_client, module_loaded, ws_key, actions_key):
        self._setup_scenario(redis_client, ws_key, actions_key)
        raw = redis_client.execute_command([
            "CYGOAP.PLAN", ws_key, actions_key, "door_open", "1",
        ])
        assert raw is not None
        plan = [str(a) for a in raw]
        assert len(plan) == 2
        assert plan[0] == "pickup_key"
        assert plan[1] == "open_door"

    def test_plan_impossible_goal(self, redis_client, module_loaded, ws_key, actions_key):
        redis_client.execute_command([
            "CYGOAP.SETSTATE", ws_key, "alive", "1",
        ])
        redis_client.execute_command([
            "CYGOAP.DEFACTION", actions_key, "noop",
            '{"alive": true}', '{"alive": true}', "1",
        ])
        raw = redis_client.execute_command([
            "CYGOAP.PLAN", ws_key, actions_key, "has_jetpack", "1",
        ])
        assert raw == [] or raw is None

    def test_plan_max_depth_1_truncates(self, redis_client, module_loaded, ws_key, actions_key):
        self._setup_scenario(redis_client, ws_key, actions_key)
        raw = redis_client.execute_command([
            "CYGOAP.PLAN", ws_key, actions_key, "door_open", "1", "1",
        ])
        # Depth 1 can only do 1 action; door_open needs 2 → no plan
        assert raw == [] or raw is None


@pytest.mark.redis
class TestCyGOAPApply:
    def test_apply_success(self, redis_client, module_loaded, ws_key, actions_key):
        redis_client.execute_command([
            "CYGOAP.SETSTATE", ws_key, "near_key", "1", "has_key", "0",
        ])
        redis_client.execute_command([
            "CYGOAP.DEFACTION", actions_key, "pickup_key",
            '{"near_key": true}', '{"has_key": true}', "1",
        ])
        raw = redis_client.execute_command([
            "CYGOAP.APPLY", ws_key, actions_key, "pickup_key",
        ])
        assert str(raw[0]) == "applied"
        assert redis_client.execute_command(["HGET", ws_key, "has_key"]) == "1"

    def test_apply_precondition_fails(self, redis_client, module_loaded, ws_key, actions_key):
        redis_client.execute_command([
            "CYGOAP.SETSTATE", ws_key, "near_key", "0", "has_key", "0",
        ])
        redis_client.execute_command([
            "CYGOAP.DEFACTION", actions_key, "pickup_key",
            '{"near_key": true}', '{"has_key": true}', "1",
        ])
        raw = redis_client.execute_command([
            "CYGOAP.APPLY", ws_key, actions_key, "pickup_key",
        ])
        assert str(raw[0]) == "failed"


@pytest.mark.redis
class TestCyGOAPWrapper:
    def test_wrapper_plan_roundtrip(self, redis_client, module_loaded, ws_key, actions_key):
        try:
            from cyredis_game.goap import CyGOAP
        except ImportError:
            pytest.skip("CyGOAP Cython extension not built")

        goap = CyGOAP(redis_client, ws_key, actions_key)
        goap.set_state({"near_item": True, "has_item": False, "used_item": False})
        goap.define_action("pick_up",  {"near_item": True},  {"has_item": True},  1)
        goap.define_action("use_item", {"has_item": True},   {"used_item": True}, 1)

        plan = goap.plan("used_item", "1")
        assert plan == ["pick_up", "use_item"]

        state = goap.get_state()
        assert state.get("near_item") is True
        assert state.get("has_item")  is False
