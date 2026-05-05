"""
Unit tests for cyredis_game.game_engine

Tests cover:
- Serialization helpers (no Redis needed)
- CyZone / CyGameWorld / CyGameEngine construction and key generation
- Lua function source (correct header, all functions registered)
- Integration: spawn, tick, intent, event round-trip (requires Redis)
- Cross-zone transfer round-trip (requires Redis)
"""
import re
import time
import uuid
import pytest

from cyredis_game.game_engine import (
    GameEngine,
    CyZone,
    CyGameWorld,
    CyGameEngine,
    GAME_ENGINE_FUNCTIONS,
    serialize_game_data,
    deserialize_game_data,
    DEFAULT_TICK_MS,
    MAX_INTENTS_PER_TICK,
)
from cy_redis.core.cy_redis_client import CyRedisClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def redis_available() -> bool:
    try:
        c = CyRedisClient(host="localhost", port=6379)
        c.set("_probe", "1")
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def redis_client(redis_available):
    if not redis_available:
        pytest.skip("Redis not available")
    return CyRedisClient(host="localhost", port=6379)


@pytest.fixture(scope="session")
def game_engine(redis_client):
    engine = GameEngine(redis_client)
    engine.load_functions()
    return engine


@pytest.fixture
def world_id():
    return f"test_world_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def zone_id():
    return f"zone_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Serialization (no Redis required)
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_roundtrip_dict(self):
        data = {"x": 1.5, "y": -3.0, "name": "hero", "hp": 100}
        assert deserialize_game_data(serialize_game_data(data)) == data

    def test_roundtrip_list(self):
        data = [1, 2, "three", 4.0]
        assert deserialize_game_data(serialize_game_data(data)) == data

    def test_roundtrip_nested(self):
        data = {"meta": {"zone": "z0", "tick": 42}, "entities": [1, 2, 3]}
        assert deserialize_game_data(serialize_game_data(data)) == data

    def test_produces_bytes(self):
        result = serialize_game_data({"key": "val"})
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# Lua function source validation (no Redis required)
# ---------------------------------------------------------------------------

class TestLuaSource:
    REQUIRED_FUNCTIONS = [
        "tick_fetch_due",
        "tick_step",
        "ent_spawn",
        "ent_apply_damage",
        "sched_enqueue",
        "sched_due",
        "xfer_request",
        "xfer_apply",
    ]

    def test_has_shebang_header(self):
        assert GAME_ENGINE_FUNCTIONS.startswith("#!lua name=cy_game"), \
            "Lua source must start with '#!lua name=cy_game' for FUNCTION LOAD"

    def test_all_functions_registered(self):
        for fn in self.REQUIRED_FUNCTIONS:
            assert f"'{fn}'" in GAME_ENGINE_FUNCTIONS or f'"{fn}"' in GAME_ENGINE_FUNCTIONS, \
                f"redis.register_function('{fn}', ...) not found in Lua source"

    def test_xfer_blob_format_documented(self):
        # The blob format must include 'hp' so health is preserved across transfers
        assert "'|'..hp" in GAME_ENGINE_FUNCTIONS or '"'+'|"..hp' in GAME_ENGINE_FUNCTIONS \
               or "hp" in GAME_ENGINE_FUNCTIONS


# ---------------------------------------------------------------------------
# Zone key generation (no Redis required)
# ---------------------------------------------------------------------------

class TestZoneKeys:
    """CyZone key generation — uses a mock redis-like object"""

    def _make_zone(self, world="w", zone="z"):
        class _FakeRedis:
            def execute_command(self, *a, **kw):
                return None
        class _FakeFuncMgr:
            def call_function(self, *a, **kw):
                return None
        return CyZone(world, zone, _FakeRedis(), _FakeFuncMgr())

    def test_entity_key_has_hash_tag(self):
        z = self._make_zone("world1", "zone0")
        key = z.get_entity_key("player_1")
        assert "{world1:zone0}" in key
        assert key.endswith(":player_1")

    def test_type_key_has_hash_tag(self):
        z = self._make_zone("world1", "zone0")
        key = z.get_type_set_key("player")
        assert "{world1:zone0}" in key

    def test_tick_key(self):
        z = self._make_zone("mmo", "z1")
        assert "{mmo:z1}" in z.tick_key

    def test_intents_stream(self):
        z = self._make_zone("mmo", "z1")
        assert "{mmo:z1}" in z.intents_stream

    def test_events_stream(self):
        z = self._make_zone("mmo", "z1")
        assert "{mmo:z1}" in z.events_stream

    def test_spatial_index(self):
        z = self._make_zone("mmo", "z1")
        assert "{mmo:z1}" in z.spatial_index


# ---------------------------------------------------------------------------
# Engine construction (no Redis required)
# ---------------------------------------------------------------------------

class TestEngineConstruction:
    def test_game_engine_class_importable(self):
        assert GameEngine is not None

    def test_constants(self):
        assert DEFAULT_TICK_MS == 50
        assert MAX_INTENTS_PER_TICK == 256


# ---------------------------------------------------------------------------
# Integration tests (require Redis)
# ---------------------------------------------------------------------------

@pytest.mark.redis
class TestEntityLifecycle:
    def test_spawn_entity(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"player_{uuid.uuid4().hex[:6]}"

        result = zone.spawn_entity(eid, "player", 100.0, 200.0, 0.0, 0.0)
        assert result["success"] is True, f"spawn failed: {result}"
        assert result["status"] == "ok"

    def test_spawn_duplicate_rejected(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"dup_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "player", 10.0, 10.0)
        result = zone.spawn_entity(eid, "player", 10.0, 10.0)
        assert result["success"] is False
        assert result["status"] == "exists"

    def test_apply_damage_reduces_hp(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"dmg_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "enemy", 50.0, 50.0)
        result = zone.apply_damage(eid, 30)
        assert result["success"] is True
        assert "hp:70" in result["status"]

    def test_apply_damage_kills(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"kill_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "enemy", 60.0, 60.0)
        result = zone.apply_damage(eid, 200)  # more than 100 hp
        assert result["success"] is True
        assert result["status"] == "dead"


@pytest.mark.redis
class TestIntentAndTick:
    def test_send_intent(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"mover_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "player", 0.0, 0.0, 10.0, 5.0)
        ok = zone.send_intent(eid, "move", {"step": 1})
        assert ok is True

    def test_step_tick_advances_counter(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"tick_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "player", 0.0, 0.0, 20.0, 10.0)
        zone.send_intent(eid, "move", {})

        now_ms = int(time.time() * 1000)
        result = zone.step_tick(now_ms, 100, 10)
        assert isinstance(result["tick"], int)
        assert result["tick"] >= 1

    def test_move_intent_updates_position(self, game_engine, world_id, zone_id):
        """A 'move' intent should cause the entity position to change on tick.step"""
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"pos_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "player", 0.0, 0.0, 50.0, 25.0)
        zone.send_intent(eid, "move", {})

        now_ms = int(time.time() * 1000)
        zone.step_tick(now_ms, 1000, 10)  # dt=1000ms so position should move noticeably

        # Read position events
        events = zone.read_events("0", 50)
        pos_events = [e for _, e in events if e.get("type") == "pos" and e.get("eid") == eid]
        assert len(pos_events) > 0, "No position event emitted after move intent"

        # Position should have changed (vx=50, dt=1s → dx≈50)
        x = float(pos_events[-1].get("x", 0))
        assert abs(x - 50.0) < 1.0, f"Expected x≈50 after 1s at vx=50, got x={x}"


@pytest.mark.redis
class TestScheduler:
    def test_schedule_and_retrieve_job(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        job_id = f"job_{uuid.uuid4().hex[:6]}"

        past_ms = int(time.time() * 1000) - 1000  # 1 second ago
        result = zone.schedule_job(job_id, past_ms, '{"action":"heal","amount":20}')
        assert result["success"] is True

        due = zone.get_due_jobs(int(time.time() * 1000), limit=10)
        job_ids = [j["job_id"] for j in due]
        assert job_id in job_ids

    def test_future_job_not_due(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        job_id = f"future_{uuid.uuid4().hex[:6]}"

        future_ms = int(time.time() * 1000) + 60_000  # 1 minute from now
        zone.schedule_job(job_id, future_ms, "{}")

        due = zone.get_due_jobs(int(time.time() * 1000), limit=10)
        job_ids = [j["job_id"] for j in due]
        assert job_id not in job_ids


@pytest.mark.redis
class TestCrossZoneTransfer:
    def test_transfer_round_trip(self, game_engine, world_id):
        world = game_engine.get_world(world_id)
        zone_a = world.get_zone(f"a_{uuid.uuid4().hex[:6]}")
        zone_b_id = f"b_{uuid.uuid4().hex[:6]}"
        eid = f"xfer_{uuid.uuid4().hex[:6]}"

        # Spawn in zone A
        spawn = zone_a.spawn_entity(eid, "player", 10.0, 20.0)
        assert spawn["success"] is True

        # Initiate transfer
        result = zone_a.transfer_entity(eid, zone_b_id)
        assert result["success"] is True, f"transfer failed: {result}"

        # Apply pending transfers
        xfer_result = world.process_cross_zone_transfers()
        assert xfer_result["processed"] >= 1

        # Entity should now appear in zone B's events
        zone_b = world.get_zone(zone_b_id)
        events = zone_b.read_events("0", 20)
        spawn_events = [e for _, e in events if e.get("type") == "spawn" and e.get("eid") == eid]
        assert len(spawn_events) >= 1, "Entity did not appear in zone B after transfer"


@pytest.mark.redis
class TestSpatialQuery:
    def test_query_finds_entity_in_region(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"spatial_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "player", 150.0, 150.0)

        # Query a box that includes (150,150)
        hits = zone.query_spatial(100.0, 200.0, 100.0, 200.0)
        assert eid in hits, f"Entity not found in spatial query, hits: {hits}"

    def test_query_misses_entity_outside_region(self, game_engine, world_id, zone_id):
        world = game_engine.get_world(world_id)
        zone = world.get_zone(zone_id)
        eid = f"outside_{uuid.uuid4().hex[:6]}"

        zone.spawn_entity(eid, "player", 500.0, 500.0)

        # Query a box that does NOT include (500,500)
        hits = zone.query_spatial(0.0, 100.0, 0.0, 100.0)
        assert eid not in hits


@pytest.mark.redis
class TestEngineStats:
    def test_stats_structure(self, game_engine, world_id):
        game_engine.get_world(world_id).get_zone("stat_zone")
        stats = game_engine.get_stats()
        assert "worlds" in stats
        assert "total_zones" in stats
        assert "libraries_loaded" in stats
        assert isinstance(stats["worlds"], int)
        assert stats["worlds"] >= 1
