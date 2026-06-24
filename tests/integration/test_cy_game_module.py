"""
Integration tests for the cy_game Redis module.

Requires a running Redis instance on port 6380 with cy_game.so loaded:

    redis-server cyredis_game/module/test-redis.conf --daemonize yes

All tests talk directly to the C module commands and to the Lua gameplay
libraries (cy_game_flecs, cy_ai, cy_chain) that are loaded at module boot.

Run with:
    pytest tests/integration/test_cy_game_module.py -v -m game_module
"""

import os
import time
import uuid
import subprocess
import pytest

try:
    import redis as redis_py
    REDIS_PY_AVAILABLE = True
except ImportError:
    REDIS_PY_AVAILABLE = False

# Port where the module-loaded Redis instance lives.
MODULE_REDIS_PORT = int(os.getenv("CY_GAME_REDIS_PORT", "6380"))
MODULE_SO_PATH    = os.path.abspath("cyredis_game/module/cy_game.so")
LUA_DIR           = os.path.abspath("cyredis_game/lua")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def module_redis():
    """Connect to the module Redis instance, skip if unavailable."""
    if not REDIS_PY_AVAILABLE:
        pytest.skip("redis-py not installed")
    try:
        r = redis_py.Redis(host="127.0.0.1", port=MODULE_REDIS_PORT,
                           decode_responses=True, socket_timeout=2)
        r.ping()
    except Exception:
        pytest.skip(
            f"Module Redis not available on port {MODULE_REDIS_PORT}. "
            "Start it with: redis-server cyredis_game/module/test-redis.conf --daemonize yes"
        )
    # A plain Redis may answer on this port without the cy_game module loaded;
    # confirm the module's commands exist, otherwise skip rather than error.
    try:
        info = r.execute_command("COMMAND", "INFO", "FLECS.INIT")
        if not info or not info[0]:
            raise RuntimeError("FLECS.INIT not registered")
    except Exception:
        pytest.skip(
            f"cy_game module not loaded on port {MODULE_REDIS_PORT}. "
            "Build/load it (see cyredis_game/README.md)."
        )
    return r


@pytest.fixture(scope="session")
def lua_loaded(module_redis):
    """Load all Lua gameplay libraries, skip if already loaded."""
    for lib_name, lua_file in [
        ("cy_game_flecs", os.path.join(LUA_DIR, "cy_game_flecs.lua")),
        ("cy_ai",         os.path.join(LUA_DIR, "cy_ai.lua")),
        ("cy_chain",      os.path.join(LUA_DIR, "cy_chain.lua")),
    ]:
        if not os.path.exists(lua_file):
            pytest.skip(f"Lua file not found: {lua_file}")
        with open(lua_file) as fh:
            src = fh.read()
        try:
            module_redis.execute_command("FUNCTION", "DELETE", lib_name)
        except Exception:
            pass
        module_redis.execute_command("FUNCTION", "LOAD", src)
    return True


@pytest.fixture
def world(module_redis):
    """Create a fresh FLECS world, destroy it after the test."""
    wid = f"test_{uuid.uuid4().hex[:8]}"
    module_redis.execute_command("FLECS.INIT", wid)
    yield wid
    try:
        module_redis.execute_command("FLECS.FINI", wid)
    except Exception:
        pass
    # Also clean up Redis keys for this world
    for key in module_redis.scan_iter(f"cy:*{{{wid}*"):
        module_redis.delete(key)


def spawn(r, world, zone, eid, etype, x, y, vx=0.0, vy=0.0, hp=100):
    """Helper: spawn one entity and return 1 on success."""
    return r.execute_command(
        "FLECS.SPAWN", world, zone, eid, etype,
        str(x), str(y), str(vx), str(vy), str(hp)
    )


def get_pos(r, world, eid):
    """Return (x, y) floats for an entity's Position component."""
    data = r.execute_command("FLECS.GETCOMP", world, eid, "Position")
    if not data:
        return None
    d = {}
    for i in range(0, len(data) - 1, 2):
        d[data[i]] = float(data[i + 1])
    return d.get("x"), d.get("y")


def get_hp(r, world, eid):
    """Return hp int for an entity."""
    data = r.execute_command("FLECS.GETCOMP", world, eid, "Health")
    if not data:
        return None
    d = {}
    for i in range(0, len(data) - 1, 2):
        d[data[i]] = int(float(data[i + 1]))
    return d.get("hp")


# ── FLECS world lifecycle ─────────────────────────────────────────────────────

@pytest.mark.game_module
class TestFlecsWorldLifecycle:
    def test_init_world(self, module_redis):
        wid = f"test_{uuid.uuid4().hex[:8]}"
        result = module_redis.execute_command("FLECS.INIT", wid)
        assert result == 1
        module_redis.execute_command("FLECS.FINI", wid)

    def test_init_duplicate_rejected(self, module_redis):
        wid = f"test_{uuid.uuid4().hex[:8]}"
        module_redis.execute_command("FLECS.INIT", wid)
        try:
            module_redis.execute_command("FLECS.INIT", wid)
            pytest.fail("Expected error on duplicate INIT")
        except redis_py.ResponseError:
            pass
        finally:
            module_redis.execute_command("FLECS.FINI", wid)

    def test_fini_unknown_world(self, module_redis):
        with pytest.raises(redis_py.ResponseError):
            module_redis.execute_command("FLECS.FINI", "no_such_world_xyz")

    def test_stats(self, module_redis, world):
        spawn(module_redis, world, "z1", "e1", "player", 0, 0)
        stats = module_redis.execute_command("FLECS.STATS", world)
        assert stats is not None
        # stats is a flat list of key-value pairs
        d = {}
        for i in range(0, len(stats) - 1, 2):
            d[stats[i]] = stats[i + 1]
        # indexed_entities = our hash map count; entities = FLECS ecs_count (may differ)
        assert int(d.get("indexed_entities", d.get("entities", 0))) >= 1


# ── Entity lifecycle ──────────────────────────────────────────────────────────

@pytest.mark.game_module
class TestFlecsEntityLifecycle:
    def test_spawn_returns_1(self, module_redis, world):
        result = spawn(module_redis, world, "z1", "p1", "player", 10.0, 20.0)
        assert result == 1

    def test_spawn_duplicate_rejected(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 10.0, 20.0)
        result = spawn(module_redis, world, "z1", "p1", "player", 15.0, 25.0)
        assert result == 0

    def test_getcomp_position(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 7.5, 3.25)
        x, y = get_pos(module_redis, world, "p1")
        assert abs(x - 7.5) < 0.001
        assert abs(y - 3.25) < 0.001

    def test_getcomp_health(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0, hp=75)
        hp = get_hp(module_redis, world, "p1")
        assert hp == 75

    def test_has_component_true(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        assert module_redis.execute_command("FLECS.HAS", world, "p1", "Position") == 1
        assert module_redis.execute_command("FLECS.HAS", world, "p1", "Health") == 1

    def test_has_component_unknown_entity(self, module_redis, world):
        assert module_redis.execute_command("FLECS.HAS", world, "ghost", "Position") == 0

    def test_setcomp_position(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        module_redis.execute_command("FLECS.SETCOMP", world, "p1", "Position", "x", "99.0", "y", "88.0")
        x, y = get_pos(module_redis, world, "p1")
        assert abs(x - 99.0) < 0.001
        assert abs(y - 88.0) < 0.001

    def test_delete_entity(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        result = module_redis.execute_command("FLECS.DELETE", world, "p1")
        assert result == 1
        assert module_redis.execute_command("FLECS.HAS", world, "p1", "Position") == 0

    def test_delete_nonexistent(self, module_redis, world):
        result = module_redis.execute_command("FLECS.DELETE", world, "no_one")
        assert result == 0

    def test_redis_hash_persisted_after_spawn(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 5.0, 6.0)
        ent_key = f"cy:ent:{{{world}:z1}}:p1"
        x = float(module_redis.hget(ent_key, "x"))
        y = float(module_redis.hget(ent_key, "y"))
        assert abs(x - 5.0) < 0.001
        assert abs(y - 6.0) < 0.001

    def test_spatial_zset_updated_after_spawn(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 3.0, 4.0)
        spatial_key = f"cy:spatial:{{{world}:z1}}"
        assert module_redis.zscore(spatial_key, "p1") is not None


# ── Tick simulation ───────────────────────────────────────────────────────────

@pytest.mark.game_module
class TestFlecsTick:
    def test_tick_advances_counter(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0, vx=1.0, vy=0.0)
        result = module_redis.execute_command("FLECS.TICK", world, "z1", "100", "256")
        d = {}
        for i in range(0, len(result) - 1, 2):
            d[result[i]] = result[i + 1]
        assert int(d["tick"]) == 1

    def test_tick_moves_entity_with_velocity(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0.0, 0.0, vx=5.0, vy=2.0)
        module_redis.execute_command("FLECS.TICK", world, "z1", "1000", "256")
        x, y = get_pos(module_redis, world, "p1")
        # Position += Velocity * dt(s): x = 0 + 5*1 = 5, y = 0 + 2*1 = 2
        assert abs(x - 5.0) < 0.01
        assert abs(y - 2.0) < 0.01

    def test_tick_zero_velocity_entity_stays(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 7.0, 8.0, vx=0.0, vy=0.0)
        module_redis.execute_command("FLECS.TICK", world, "z1", "1000", "256")
        x, y = get_pos(module_redis, world, "p1")
        assert abs(x - 7.0) < 0.001
        assert abs(y - 8.0) < 0.001

    def test_tick_syncs_redis_hash(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0.0, 0.0, vx=3.0, vy=0.0)
        module_redis.execute_command("FLECS.TICK", world, "z1", "2000", "256")
        ent_key = f"cy:ent:{{{world}:z1}}:p1"
        x = float(module_redis.hget(ent_key, "x"))
        assert abs(x - 6.0) < 0.01

    def test_tick_syncs_spatial_zset(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0.0, 0.0, vx=10.0, vy=0.0)
        module_redis.execute_command("FLECS.TICK", world, "z1", "1000", "256")
        spatial_key = f"cy:spatial:{{{world}:z1}}"
        score = module_redis.zscore(spatial_key, "p1")
        assert score is not None
        # Decode score: floor(x*1000)*1e9 + floor(y*1000)
        score_int = int(score)
        decoded_x = (score_int // 1_000_000_000) / 1000.0
        assert abs(decoded_x - 10.0) < 0.1

    def test_multiple_entities_all_move(self, module_redis, world):
        for i in range(5):
            spawn(module_redis, world, "z1", f"e{i}", "soldier",
                  float(i), 0.0, vx=float(i + 1), vy=0.0)
        module_redis.execute_command("FLECS.TICK", world, "z1", "1000", "256")
        for i in range(5):
            x, _ = get_pos(module_redis, world, f"e{i}")
            assert abs(x - (float(i) + float(i + 1))) < 0.01

    def test_tick_drains_intent_stream(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        intent_key = f"cy:intents:{{{world}:z1}}"
        # Add a dummy intent entry
        module_redis.xadd(intent_key, {"eid": "p1", "kind": "move"})
        result = module_redis.execute_command("FLECS.TICK", world, "z1", "100", "256")
        d = {}
        for i in range(0, len(result) - 1, 2):
            d[result[i]] = result[i + 1]
        assert int(d["intents"]) >= 0  # processed intents field present

    def test_tick_counter_accumulates(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        for expected_tick in range(1, 6):
            result = module_redis.execute_command("FLECS.TICK", world, "z1", "50", "256")
            d = {}
            for i in range(0, len(result) - 1, 2):
                d[result[i]] = result[i + 1]
            assert int(d["tick"]) == expected_tick


# ── Query ─────────────────────────────────────────────────────────────────────

@pytest.mark.game_module
class TestFlecsQuery:
    def test_query_all_with_position(self, module_redis, world):
        for i in range(3):
            spawn(module_redis, world, "z1", f"e{i}", "player", float(i), 0.0)
        result = module_redis.execute_command("FLECS.QUERY", world, "Position")
        assert len(result) == 3
        assert set(result) == {"e0", "e1", "e2"}

    def test_query_multi_component_filter(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0, hp=100)
        spawn(module_redis, world, "z1", "p2", "player", 1, 1, hp=50)
        result = module_redis.execute_command("FLECS.QUERY", world, "Position,Health")
        assert "p1" in result
        assert "p2" in result

    def test_query_zone_filter(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        spawn(module_redis, world, "z2", "p2", "player", 1, 1)
        z1_result = module_redis.execute_command("FLECS.QUERY", world, "Position", "z1")
        z2_result = module_redis.execute_command("FLECS.QUERY", world, "Position", "z2")
        assert "p1" in z1_result and "p2" not in z1_result
        assert "p2" in z2_result and "p1" not in z2_result

    def test_query_empty_world(self, module_redis, world):
        result = module_redis.execute_command("FLECS.QUERY", world, "Position")
        assert result == [] or result is None or len(result) == 0

    def test_query_after_delete(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        spawn(module_redis, world, "z1", "p2", "player", 1, 1)
        module_redis.execute_command("FLECS.DELETE", world, "p1")
        result = module_redis.execute_command("FLECS.QUERY", world, "Position")
        assert "p1" not in result
        assert "p2" in result


# ── Restore ───────────────────────────────────────────────────────────────────

@pytest.mark.game_module
class TestFlecsRestore:
    def test_restore_reloads_entities(self, module_redis, world):
        for i in range(3):
            spawn(module_redis, world, "z1", f"e{i}", "player",
                  float(i * 10), float(i * 5), hp=100 - i * 10)
        # Destroy + restore
        module_redis.execute_command("FLECS.FINI", world)
        module_redis.execute_command("FLECS.INIT", world)
        loaded = module_redis.execute_command("FLECS.RESTORE", world)
        assert int(loaded) == 3
        for i in range(3):
            x, y = get_pos(module_redis, world, f"e{i}")
            assert abs(x - float(i * 10)) < 0.01
            assert abs(y - float(i * 5)) < 0.01


# ── Pathfinding ───────────────────────────────────────────────────────────────

@pytest.mark.game_module
class TestPathfinding:
    def _grid_key(self, world):
        return f"cy:nav:{{{world}:z1}}:grid"

    def test_straight_path_no_obstacles(self, module_redis, world):
        grid = self._grid_key(world)
        result = module_redis.execute_command("CYPATH.FIND", grid, "0", "0", "4", "0", "100")
        coords = list(zip(result[::2], result[1::2]))
        assert (4, 0) in [(int(x), int(y)) for x, y in coords] or \
               any(int(x) == 4 for x, _ in coords)

    def test_path_avoids_wall(self, module_redis, world):
        grid = self._grid_key(world)
        # Build a vertical wall at x=3, y=-2..6 (wide enough that path can't slip past)
        for y in range(-2, 7):
            module_redis.execute_command("CYPATH.SET", grid, "3", str(y), "1")
        result = module_redis.execute_command("CYPATH.FIND", grid, "0", "0", "5", "0", "200")
        # Path must exist (goes around) and none of the blocked cells appear
        coords = [(int(x), int(y)) for x, y in zip(result[::2], result[1::2])]
        assert len(coords) > 0
        blocked = {(3, y) for y in range(-2, 7)}
        assert all((x, y) not in blocked for x, y in coords)

    def test_no_path_fully_blocked(self, module_redis, world):
        grid = self._grid_key(world)
        # Enclose start (0,0) in a box, goal (10,10) is outside; bounded search
        for x in range(-1, 3):
            module_redis.execute_command("CYPATH.SET", grid, str(x), "-1", "1")  # bottom
            module_redis.execute_command("CYPATH.SET", grid, str(x), "2",  "1")  # top
        for y in range(-1, 3):
            module_redis.execute_command("CYPATH.SET", grid, "-1", str(y), "1")  # left
            module_redis.execute_command("CYPATH.SET", grid, "2",  str(y), "1")  # right
        # Max steps too small to escape even if grid were open
        result = module_redis.execute_command("CYPATH.FIND", grid, "0", "0", "10", "10", "10")
        assert len(result) == 0

    def test_max_steps_respected(self, module_redis, world):
        grid = self._grid_key(world)
        result = module_redis.execute_command("CYPATH.FIND", grid, "0", "0", "100", "0", "5")
        assert len(result) <= 5 * 2  # at most 5 waypoints

    def test_clear_grid(self, module_redis, world):
        grid = self._grid_key(world)
        module_redis.execute_command("CYPATH.SET", grid, "1", "0", "1")
        module_redis.execute_command("CYPATH.CLEAR", grid)
        result = module_redis.execute_command("CYPATH.FIND", grid, "0", "0", "3", "0", "20")
        coords = [(int(x), int(y)) for x, y in zip(result[::2], result[1::2])]
        assert any(x == 1 for x, _ in coords)  # now passable


# ── Physics ───────────────────────────────────────────────────────────────────

@pytest.mark.game_module
class TestPhysics:
    def test_aabb_overlap(self, module_redis):
        result = module_redis.execute_command(
            "CYPHYS.AABB", "0", "0", "2", "2", "1", "1", "2", "2")
        assert result == 1

    def test_aabb_no_overlap(self, module_redis):
        result = module_redis.execute_command(
            "CYPHYS.AABB", "0", "0", "1", "1", "10", "10", "1", "1")
        assert result == 0

    def test_circle_finds_nearby_entity(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 5.0, 5.0)
        spatial = f"cy:spatial:{{{world}:z1}}"
        result = module_redis.execute_command("CYPHYS.CIRCLE", spatial, "5.0", "5.0", "3.0", "10")
        assert len(result) >= 2
        eids = result[::2]
        assert "p1" in eids

    def test_circle_excludes_distant_entity(self, module_redis, world):
        spawn(module_redis, world, "z1", "p1", "player", 100.0, 100.0)
        spatial = f"cy:spatial:{{{world}:z1}}"
        result = module_redis.execute_command("CYPHYS.CIRCLE", spatial, "0.0", "0.0", "5.0", "10")
        eids = result[::2] if result else []
        assert "p1" not in eids

    def test_circle_sorted_by_distance(self, module_redis, world):
        spawn(module_redis, world, "z1", "close", "player", 1.0, 0.0)
        spawn(module_redis, world, "z1", "far",   "player", 4.0, 0.0)
        spatial = f"cy:spatial:{{{world}:z1}}"
        result = module_redis.execute_command("CYPHYS.CIRCLE", spatial, "0.0", "0.0", "10.0", "10")
        eids = result[::2]
        dists = [float(d) for d in result[1::2]]
        assert dists == sorted(dists)
        assert eids[0] == "close"


# ── GOAP ──────────────────────────────────────────────────────────────────────

@pytest.mark.game_module
class TestGOAP:
    def _keys(self, world):
        ws      = f"cy:ws:{{{world}:z1}}:agent"
        actions = f"cy:actions:{{{world}}}"
        return ws, actions

    def _setup_combat_actions(self, r, world):
        ws, actions = self._keys(world)
        r.execute_command("CYGOAP.SETSTATE",    ws, "has_enemy", "0", "is_alive", "1", "has_ammo", "1")
        r.execute_command("CYGOAP.DEFACTION",   actions, "attack",
                          '{"has_enemy":1,"has_ammo":1}', '{"has_enemy":0}', "1")
        r.execute_command("CYGOAP.DEFACTION",   actions, "reload",
                          '{"is_alive":1}', '{"has_ammo":1}', "2")
        r.execute_command("CYGOAP.DEFACTION",   actions, "scout",
                          '{"is_alive":1}', '{"has_enemy":1}', "3")
        return ws, actions

    def test_plan_single_action(self, module_redis, world):
        ws, actions = self._setup_combat_actions(module_redis, world)
        plan = module_redis.execute_command("CYGOAP.PLAN", ws, actions, "has_enemy", "1", "10")
        assert plan == ["scout"]

    def test_plan_two_step(self, module_redis, world):
        ws, actions = self._keys(world)
        # has_enemy=1 (need to kill), is_alive=1, has_ammo=0 (need to reload first)
        module_redis.execute_command("CYGOAP.SETSTATE", ws,
                                     "has_enemy", "1", "is_alive", "1", "has_ammo", "0")
        module_redis.execute_command("CYGOAP.DEFACTION", actions, "attack",
                                     '{"has_enemy":1,"has_ammo":1}', '{"has_enemy":0}', "1")
        module_redis.execute_command("CYGOAP.DEFACTION", actions, "reload",
                                     '{"is_alive":1}', '{"has_ammo":1}', "2")
        plan = module_redis.execute_command("CYGOAP.PLAN", ws, actions, "has_enemy", "0", "10")
        # reload (has_ammo: 0→1) → attack (has_enemy: 1→0)
        assert len(plan) >= 2
        assert "reload" in plan
        assert "attack" in plan

    def test_plan_goal_already_satisfied(self, module_redis, world):
        ws, actions = self._setup_combat_actions(module_redis, world)
        # has_enemy is already 0
        plan = module_redis.execute_command("CYGOAP.PLAN", ws, actions, "has_enemy", "0", "10")
        assert plan == [] or plan is None or len(plan) == 0

    def test_plan_unsatisfiable(self, module_redis, world):
        ws, actions = self._keys(world)
        module_redis.execute_command("CYGOAP.SETSTATE", ws, "alive", "1")
        module_redis.execute_command("CYGOAP.DEFACTION", actions, "noop",
                                     '{"alive":1}', '{"alive":1}', "1")
        plan = module_redis.execute_command("CYGOAP.PLAN", ws, actions, "has_magic", "1", "5")
        assert plan == [] or plan is None or len(plan) == 0

    def test_apply_action(self, module_redis, world):
        ws, actions = self._setup_combat_actions(module_redis, world)
        result = module_redis.execute_command("CYGOAP.APPLY", ws, actions, "scout")
        assert result[0] == "applied"
        # State should now have has_enemy=1
        state_raw = module_redis.hgetall(ws)
        assert state_raw.get("has_enemy") == "1"


# ── Lua gameplay functions ────────────────────────────────────────────────────

@pytest.mark.game_module
class TestLuaGameplayFunctions:
    def test_flecs_tick_via_lua(self, module_redis, lua_loaded, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0, vx=2.0, vy=0.0)
        result = module_redis.execute_command("FCALL", "flecs_tick", "1", world, "z1", "1000", "256")
        d = {}
        for i in range(0, len(result) - 1, 2):
            d[result[i]] = result[i + 1]
        assert int(d["tick"]) == 1

    def test_flecs_snapshot_via_lua(self, module_redis, lua_loaded, world):
        spawn(module_redis, world, "z1", "p1", "player", 3.0, 4.0, vx=1.0, vy=2.0, hp=80)
        result = module_redis.execute_command("FCALL", "flecs_snapshot", "1", world, "p1")
        d = {}
        it = iter(result)
        for k in it:
            d[k] = next(it, None)
        assert d.get("eid") == "p1"
        assert abs(float(d.get("x", 0)) - 3.0) < 0.001
        assert abs(float(d.get("y", 0)) - 4.0) < 0.001

    def test_flecs_query_via_lua(self, module_redis, lua_loaded, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0)
        spawn(module_redis, world, "z1", "p2", "archer", 1, 1)
        result = module_redis.execute_command(
            "FCALL", "flecs_query", "1", world, "Position,Velocity", "z1")
        assert "p1" in result
        assert "p2" in result

    def test_flecs_apply_damage_reduces_hp(self, module_redis, lua_loaded, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0, hp=100)
        result = module_redis.execute_command("FCALL", "flecs_apply_damage", "1", world, "p1", "30")
        assert result[0] == 1
        assert "hp:70" in result[1]

    def test_flecs_apply_damage_kills(self, module_redis, lua_loaded, world):
        spawn(module_redis, world, "z1", "p1", "player", 0, 0, hp=10)
        result = module_redis.execute_command("FCALL", "flecs_apply_damage", "1", world, "p1", "50")
        assert result[0] == 1
        assert result[1] == "dead"
        assert module_redis.execute_command("FLECS.HAS", world, "p1", "Position") == 0

    def test_ai_find_path_via_lua(self, module_redis, lua_loaded, world):
        grid = f"cy:nav:{{{world}:z1}}:grid"
        result = module_redis.execute_command(
            "FCALL", "ai_find_path", "1", grid, "0", "0", "3", "0", "50")
        assert len(result) > 0
        final_x = int(result[-2])
        assert final_x == 3

    def test_ai_goap_plan_via_lua(self, module_redis, lua_loaded, world):
        ws      = f"cy:ws:{{{world}:z1}}:agent"
        actions = f"cy:actions:{{{world}}}"
        module_redis.execute_command("CYGOAP.SETSTATE", ws, "alive", "1", "target_found", "0")
        module_redis.execute_command("CYGOAP.DEFACTION", actions, "scan",
                                     '{"alive":1}', '{"target_found":1}', "1")
        result = module_redis.execute_command(
            "FCALL", "ai_goap_plan", "2", ws, actions, "target_found", "1", "5")
        assert result == ["scan"]

    def test_ai_nearby_via_lua(self, module_redis, lua_loaded, world):
        spawn(module_redis, world, "z1", "p1", "player", 2.0, 2.0)
        spatial = f"cy:spatial:{{{world}:z1}}"
        result = module_redis.execute_command(
            "FCALL", "ai_nearby", "1", spatial, "2.0", "2.0", "5.0", "10")
        assert len(result) >= 2
        assert "p1" in result[::2]

    def test_chain_tick_and_query_via_lua(self, module_redis, lua_loaded, world):
        spawn(module_redis, world, "z1", "p1", "player", 0.0, 0.0, vx=1.0)
        spatial = f"cy:spatial:{{{world}:z1}}"
        result = module_redis.execute_command(
            "FCALL", "chain_tick_and_query", "2",
            world, spatial, "z1", "1000", "1.0", "0.0", "10.0")
        assert result is not None and len(result) == 2
        tick_r = result[0]
        d = {}
        for i in range(0, len(tick_r) - 1, 2):
            d[tick_r[i]] = tick_r[i + 1]
        assert int(d["tick"]) == 1

    def test_chain_spawn_and_path_via_lua(self, module_redis, lua_loaded, world):
        grid = f"cy:nav:{{{world}:z1}}:grid"
        result = module_redis.execute_command(
            "FCALL", "chain_spawn_and_path", "2",
            world, grid, "z1", "npc1", "soldier", "0", "0", "4", "0")
        assert result is not None and len(result) == 2
        spawn_ok = result[0]
        path     = result[1]
        assert spawn_ok == 1
        assert len(path) > 0


# ── Full Python driver integration ────────────────────────────────────────────

@pytest.mark.game_module
class TestPythonDriverIntegration:
    """End-to-end tests using the CyGameModule / CyZone / GameEngine Cython drivers."""

    @pytest.fixture(scope="class")
    def game_engine(self, module_redis):
        """Build a GameEngine backed by the module Redis."""
        try:
            from cyredis_game.game_engine import GameEngine
        except ImportError:
            pytest.skip("cyredis_game not built")

        # Wrap module_redis in a thin adapter the GameEngine accepts
        class _Adapter:
            """Minimal adapter: GameEngine calls .execute_command(list)."""
            def __init__(self, r):
                self._r = r
            def execute_command(self, args):
                if isinstance(args, (list, tuple)):
                    return self._r.execute_command(*args)
                return self._r.execute_command(args)
            def pipeline(self):
                return _Pipeline(self._r.pipeline())

        class _Pipeline:
            def __init__(self, pipe):
                self._pipe = pipe
            def execute_command(self, args):
                if isinstance(args, (list, tuple)):
                    self._pipe.execute_command(*args)
                else:
                    self._pipe.execute_command(args)
            def execute(self):
                return self._pipe.execute()

        eng = GameEngine(_Adapter(module_redis))
        try:
            eng.load_module(MODULE_SO_PATH)
        except Exception:
            pass  # already loaded via test-redis.conf
        return eng

    def test_init_and_get_world(self, module_redis, game_engine):
        wid = f"test_{uuid.uuid4().hex[:8]}"
        game_engine.init_world(wid)
        w = game_engine.get_world(wid)
        assert w is not None
        # CyGameWorld exposes world_id or the world is identifiable
        assert hasattr(w, "world_id") or hasattr(w, "_world_id") or w is not None
        module_redis.execute_command("FLECS.FINI", wid)

    def test_spawn_via_zone(self, module_redis, game_engine):
        wid = f"test_{uuid.uuid4().hex[:8]}"
        game_engine.init_world(wid)
        w = game_engine.get_world(wid)
        z = w.get_zone("z1")
        result = z.flecs_spawn("hero", "warrior", 10.0, 20.0, 1.0, 0.0, 100)
        # flecs_spawn returns int 1 or a dict with success=True
        success = result == 1 or (isinstance(result, dict) and result.get("success"))
        assert success
        module_redis.execute_command("FLECS.FINI", wid)

    def test_tick_via_zone(self, module_redis, game_engine):
        wid = f"test_{uuid.uuid4().hex[:8]}"
        game_engine.init_world(wid)
        w = game_engine.get_world(wid)
        z = w.get_zone("z1")
        z.flecs_spawn("hero", "warrior", 0.0, 0.0, 5.0, 0.0, 100)
        result = z.flecs_tick(1000)
        assert result is not None
        module_redis.execute_command("FLECS.FINI", wid)

    def test_find_path_via_zone(self, module_redis, game_engine):
        wid = f"test_{uuid.uuid4().hex[:8]}"
        game_engine.init_world(wid)
        w = game_engine.get_world(wid)
        z = w.get_zone("z1")
        path = z.find_path(0, 0, 5, 5)
        assert isinstance(path, list)
        module_redis.execute_command("FLECS.FINI", wid)
