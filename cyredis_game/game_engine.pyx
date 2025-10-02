# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
CyRedis Game Engine - Authoritative ECS on Redis
Distributed, server-side Entity-Component-System with zones, ticks, and scaling
"""

import json
import time
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor

# Fast binary serialization instead of JSON
try:
    import msgpack
    USE_MSGPACK = True
except ImportError:
    try:
        import umsgpack as msgpack  # Ultra fast MessagePack
        USE_MSGPACK = True
    except ImportError:
        import json as msgpack  # Fallback to JSON if msgpack not available
        USE_MSGPACK = False

# Optimized serialization functions
def serialize_game_data(data: Any) -> bytes:
    """Serialize game data using MessagePack for speed"""
    if USE_MSGPACK:
        return msgpack.packb(data, use_bin_type=True)
    else:
        return msgpack.dumps(data).encode('utf-8')

def deserialize_game_data(data: bytes) -> Any:
    """Deserialize game data using MessagePack for speed"""
    if USE_MSGPACK:
        return msgpack.unpackb(data, raw=False, strict_map_key=False)
    else:
        return msgpack.loads(data.decode('utf-8'))

# For Lua compatibility (simple data only)
def serialize_lua_data(data: dict) -> str:
    """Serialize simple dict for Lua functions (JSON for compatibility)"""
    return json.dumps(data)

def deserialize_lua_data(data: str) -> dict:
    """Deserialize simple dict from Lua functions"""
    return json.loads(data)

# Import base CyRedis components
try:
    from cy_redis.cy_redis_client import CyRedisClient
    from cy_redis.functions import CyRedisFunctionsManager
except ImportError:
    # Fallback for development
    from ..cy_redis.cy_redis_client import CyRedisClient
    from ..cy_redis.functions import CyRedisFunctionsManager

# Game engine constants
DEF DEFAULT_TICK_MS = 50
DEF MAX_INTENTS_PER_TICK = 256
DEF SPATIAL_PRECISION = 1000  # x*1000 for spatial indexing

# Game Engine Redis Functions (Lua code)
GAME_ENGINE_FUNCTIONS = """
-- cy:game - Redis Game Engine Functions

-- Tick helpers
redis.register_function('tick.fetch_due', function(keys, args)
    local k, now, tick = keys[1], tonumber(args[1]), tonumber(args[2])
    local t = tonumber(redis.call('HGET', k, 't') or 0)
    local last = tonumber(redis.call('HGET', k, 'last_ms') or 0)
    if now - last >= tick then
        return {1, t+1}
    else
        return {0, t}
    end
end)

redis.register_function('tick.step', function(keys, args)
    local kTick, kInt, kEv, kSp = keys[1], keys[2], keys[3], keys[4]
    local now, dt, budget = tonumber(args[1]), tonumber(args[2]), tonumber(args[3])
    local W, Z = args[4], args[5]

    -- 1) drain intents
    local read = redis.call('XREAD', 'COUNT', budget, 'STREAMS', kInt, '0-0')
    local consumed = 0
    if read and #read>0 then
        local entries = read[1][2]
        for i=1,#entries do
            local id = entries[i][1]
            local fields = entries[i][2]
            -- fields: eid, kind, payload...
            local eid, kind = nil, nil
            for j=1,#fields,2 do
                local f = fields[j]
                local v = fields[j+1]
                if f=='eid' then eid=v elseif f=='kind' then kind=v end
            end
            if kind=='move' and eid then
                -- minimal example: apply velocity for dt to position
                local keyEnt = 'cy:ent:{'..W..':'..Z..'}:'..eid
                local ent = redis.call('HMGET', keyEnt, 'x','y','vx','vy','type','version')
                if ent and ent[1] then
                    local x = tonumber(ent[1] or '0') + tonumber(ent[3] or '0') * dt/1000.0
                    local y = tonumber(ent[2] or '0') + tonumber(ent[4] or '0') * dt/1000.0
                    local ver = (tonumber(ent[6] or '0') + 1)
                    redis.call('HSET', keyEnt, 'x', x, 'y', y, 'version', ver)
                    -- update spatial index
                    local score = math.floor(x*1000)*1000000000 + math.floor(y*1000)
                    redis.call('ZADD', kSp, score, eid)
                    redis.call('XADD', kEv, '*', 'eid', eid, 'type', 'pos', 'x', tostring(x), 'y', tostring(y), 'ver', tostring(ver))
                end
            end
            consumed = consumed + 1
            redis.call('XDEL', kInt, id)
            if consumed >= budget then break end
        end
    end
    -- 2) simple system: decay/regen could go here, trimmed for brevity

    -- 3) advance tick clock
    local t = tonumber(redis.call('HINCRBY', kTick, 't', 1))
    redis.call('HSET', kTick, 'last_ms', now)
    return {t, consumed}
end)

-- Entity operations
redis.register_function('ent.spawn', function(keys, args)
    local kEnt, kSp, kType = keys[1], keys[2], keys[3]
    local eid, typ = args[1], args[2]
    local x, y = tonumber(args[3]), tonumber(args[4])
    local vx, vy = tonumber(args[5] or '0'), tonumber(args[6] or '0')
    if redis.call('EXISTS', kEnt)==1 then return {0, 'exists'} end
    redis.call('HSET', kEnt, 'eid', eid, 'type', typ, 'x', x, 'y', y, 'vx', vx, 'vy', vy, 'version', 1)
    local score = math.floor(x*1000)*1000000000 + math.floor(y*1000)
    redis.call('ZADD', kSp, score, eid)
    redis.call('SADD', kType, eid)
    return {1, 'ok'}
end)

redis.register_function('ent.apply_damage', function(keys, args)
    local kEnt, kEv, kType = keys[1], keys[2], keys[3]
    local dmg = tonumber(args[1])
    local hp = tonumber(redis.call('HGET', kEnt, 'hp') or '0') - dmg
    if hp <= 0 then
        local eid = redis.call('HGET', kEnt, 'eid')
        local typ = redis.call('HGET', kEnt, 'type')
        redis.call('DEL', kEnt)
        redis.call('SREM', kType, eid)
        redis.call('XADD', kEv, '*', 'eid', eid, 'type', 'death')
        return {1, 'dead'}
    else
        redis.call('HSET', kEnt, 'hp', hp)
        return {1, 'hp:'..hp}
    end
end)

-- Scheduling
redis.register_function('sched.enqueue', function(keys, args)
    local kSched, id, run_at, payload = keys[1], args[1], tonumber(args[2]), args[3]
    redis.call('ZADD', kSched, run_at, id)
    redis.call('HSET', kSched..':payload', id, payload)
    return 1
end)

redis.register_function('sched.due', function(keys, args)
    local k, now, limit = keys[1], tonumber(args[1]), tonumber(args[2])
    local ids = redis.call('ZRANGEBYSCORE', k, '-inf', now, 'LIMIT', 0, limit)
    local out = {}
    for i=1,#ids do
        local id = ids[i]
        local payload = redis.call('HGET', k..':payload', id)
        table.insert(out, id); table.insert(out, payload or '')
    end
    if #ids>0 then
        redis.call('ZREM', k, unpack(ids))
        redis.call('HDEL', k..':payload', unpack(ids))
    end
    return out
end)

-- Cross-zone transfer (optimized binary format)
redis.register_function('xfer.request', function(keys, args)
    local kEnt, kOut, toZ = keys[1], keys[2], args[1]
    local all = redis.call('HGETALL', kEnt)
    if #all==0 then return {0, 'missing'} end
    -- Create compact binary format: eid|type|x|y|vx|vy|version|toZ
    local eid = redis.call('HGET', kEnt, 'eid') or 'unknown'
    local typ = redis.call('HGET', kEnt, 'type') or 'unknown'
    local x = redis.call('HGET', kEnt, 'x') or '0'
    local y = redis.call('HGET', kEnt, 'y') or '0'
    local vx = redis.call('HGET', kEnt, 'vx') or '0'
    local vy = redis.call('HGET', kEnt, 'vy') or '0'
    local ver = redis.call('HGET', kEnt, 'version') or '1'
    -- Binary format: values separated by | for easy parsing
    local payload = eid..'|'..typ..'|'..x..'|'..y..'|'..vx..'|'..vy..'|'..ver..'|'..toZ
    redis.call('XADD', kOut, '*', 'type', 'xfer', 'blob', payload)
    redis.call('DEL', kEnt)
    return {1, 'queued'}
end)

redis.register_function('xfer.apply', function(keys, args)
    local kEv, kSp, blob = keys[1], keys[2], args[1]
    -- Parse binary format: eid|type|x|y|vx|vy|version|toZ
    local parts = {}
    for part in string.gmatch(blob, '([^|]+)') do
        table.insert(parts, part)
    end
    if #parts < 8 then return {0, 'invalid_format'} end

    local eid, typ, x, y, vx, vy, ver, toZ = unpack(parts)
    local W = args[2] or 'unknown'  -- Passed as additional arg

    local kEnt = 'cy:ent:{'..W..':'..toZ..'}:'..eid
    redis.call('HSET', kEnt, 'eid', eid, 'type', typ, 'x', x, 'y', y,
               'vx', vx, 'vy', vy, 'version', ver)
    local score = math.floor(tonumber(x)*1000)*1000000000 + math.floor(tonumber(y)*1000)
    redis.call('ZADD', kSp, score, eid)
    redis.call('XADD', kEv, '*', 'eid', eid, 'type', 'spawn', 'zone', toZ)
    return {1, 'applied'}
end)
"""

# World and Zone management
cdef class CyZone:
    """
    Represents a game zone with its own simulation space
    """

    cdef str world_id
    cdef str zone_id
    cdef CyRedisClient redis
    cdef CyRedisFunctionsManager func_mgr

    cdef str tick_key
    cdef str intents_stream
    cdef str events_stream
    cdef str spatial_index
    cdef str schedule_zset

    def __cinit__(self, str world_id, str zone_id, CyRedisClient redis, CyRedisFunctionsManager func_mgr):
        self.world_id = world_id
        self.zone_id = zone_id
        self.redis = redis
        self.func_mgr = func_mgr

        # Pre-compute key names with hash tags for cluster safety
        self.tick_key = f"cy:tick:{{{world_id}:{zone_id}}}"
        self.intents_stream = f"cy:intents:{{{world_id}:{zone_id}}}"
        self.events_stream = f"cy:events:{{{world_id}:{zone_id}}}"
        self.spatial_index = f"cy:spatial:{{{world_id}:{zone_id}}}"
        self.schedule_zset = f"cy:sched:{{{world_id}:{zone_id}}}"

    def get_entity_key(self, str entity_id):
        """Get the full entity key for this zone"""
        return f"cy:ent:{{{self.world_id}:{self.zone_id}}}:{entity_id}"

    def get_type_set_key(self, str entity_type):
        """Get the type index key"""
        return f"cy:type:{{{self.world_id}:{self.zone_id}}}:{entity_type}"

    cpdef bint is_tick_due(self, long now_ms, long tick_ms=DEFAULT_TICK_MS):
        """Check if zone tick is due"""
        result = self.func_mgr.call_function("cy:game", "tick.fetch_due",
                                           [self.tick_key], [str(now_ms), str(tick_ms)])
        return result[0] == 1 if result else False

    cpdef dict step_tick(self, long now_ms, long dt_ms=DEFAULT_TICK_MS,
                        int budget=MAX_INTENTS_PER_TICK):
        """Execute one tick step"""
        result = self.func_mgr.call_function("cy:game", "tick.step",
                                           [self.tick_key, self.intents_stream,
                                            self.events_stream, self.spatial_index],
                                           [str(now_ms), str(dt_ms), str(budget),
                                            self.world_id, self.zone_id])
        if result and len(result) >= 2:
            return {'tick': result[0], 'intents_consumed': result[1]}
        return {'tick': 0, 'intents_consumed': 0}

    cpdef dict spawn_entity(self, str entity_id, str entity_type,
                           double x, double y, double vx=0.0, double vy=0.0):
        """Spawn a new entity in this zone"""
        ent_key = self.get_entity_key(entity_id)
        type_key = self.get_type_set_key(entity_type)

        result = self.func_mgr.call_function("cy:game", "ent.spawn",
                                           [ent_key, self.spatial_index, type_key],
                                           [entity_id, entity_type, str(x), str(y), str(vx), str(vy)])

        if result and result[0] == 1:
            return {'success': True, 'status': result[1]}
        return {'success': False, 'status': result[1] if result else 'unknown'}

    cpdef dict apply_damage(self, str entity_id, int damage):
        """Apply damage to an entity"""
        ent_key = self.get_entity_key(entity_id)
        type_key = self.get_type_set_key("any")  # Would need entity type lookup

        result = self.func_mgr.call_function("cy:game", "ent.apply_damage",
                                           [ent_key, self.events_stream, type_key],
                                           [str(damage)])

        if result and result[0] == 1:
            return {'success': True, 'status': result[1]}
        return {'success': False, 'status': 'entity_not_found'}

    cpdef bint send_intent(self, str entity_id, str intent_type, payload=None):
        """Send an intent to this zone using fast binary serialization"""
        if payload is None:
            payload = {}

        now_ms = int(time.time() * 1000)

        # Use fast MessagePack serialization for payload
        try:
            payload_bytes = serialize_game_data(payload)
            payload_b64 = payload_bytes.hex()  # Use hex for Redis compatibility

            self.redis.execute_command(['XADD', self.intents_stream, '*',
                                       'eid', entity_id,
                                       'kind', intent_type,
                                       'payload', payload_b64,
                                       'ts', str(now_ms)])
            return True
        except Exception:
            return False

    cpdef list read_events(self, str last_id="0", int count=100):
        """Read events from this zone with fast binary deserialization"""
        try:
            result = self.redis.execute_command(['XREAD', 'COUNT', str(count),
                                                'STREAMS', self.events_stream, last_id])
            if result and len(result) > 0:
                events = result[0][1]  # stream entries
                # Deserialize binary payloads
                deserialized_events = []
                for event_id, event_data in events:
                    deserialized_data = {}
                    for key, value in event_data.items():
                        if key == 'payload' and value:
                            try:
                                # Deserialize from hex-encoded MessagePack
                                payload_bytes = bytes.fromhex(value)
                                deserialized_data[key] = deserialize_game_data(payload_bytes)
                            except Exception:
                                deserialized_data[key] = value  # Keep as string if deserialization fails
                        else:
                            deserialized_data[key] = value
                    deserialized_events.append((event_id, deserialized_data))
                return deserialized_events
            return []
        except Exception:
            return []

    cpdef dict schedule_job(self, str job_id, long run_at_ms, str payload=""):
        """Schedule a job for future execution"""
        try:
            result = self.func_mgr.call_function("cy:game", "sched.enqueue",
                                               [self.schedule_zset],
                                               [job_id, str(run_at_ms), payload])
            return {'success': result == 1}
        except Exception:
            return {'success': False}

    cpdef list get_due_jobs(self, long now_ms, int limit=100):
        """Get jobs that are due for execution"""
        try:
            result = self.func_mgr.call_function("cy:game", "sched.due",
                                               [self.schedule_zset],
                                               [str(now_ms), str(limit)])
            # Parse job_id, payload pairs
            jobs = []
            for i in range(0, len(result), 2):
                if i + 1 < len(result):
                    jobs.append({
                        'job_id': result[i],
                        'payload': result[i + 1]
                    })
            return jobs
        except Exception:
            return []

    cpdef dict transfer_entity(self, str entity_id, str target_zone):
        """Initiate entity transfer to another zone using optimized binary format"""
        ent_key = self.get_entity_key(entity_id)
        xfer_stream = f"cy:xmsg:{{{self.world_id}}}"

        try:
            result = self.func_mgr.call_function("cy:game", "xfer.request",
                                               [ent_key, xfer_stream],
                                               [target_zone])
            if result and result[0] == 1:
                return {'success': True, 'status': result[1]}
            return {'success': False, 'status': result[1] if result else 'unknown'}
        except Exception:
            return {'success': False, 'status': 'transfer_failed'}

# World manager
cdef class CyGameWorld:
    """
    Manages a game world with multiple zones
    """

    cdef str world_id
    cdef CyRedisClient redis
    cdef CyRedisFunctionsManager func_mgr
    cdef dict zones
    cdef str zones_list_key

    def __cinit__(self, str world_id, CyRedisClient redis, CyRedisFunctionsManager func_mgr):
        self.world_id = world_id
        self.redis = redis
        self.func_mgr = func_mgr
        self.zones = {}
        self.zones_list_key = f"cy:world:{world_id}:zones"

    cpdef CyZone get_zone(self, str zone_id):
        """Get or create a zone"""
        if zone_id not in self.zones:
            self.zones[zone_id] = CyZone(self.world_id, zone_id, self.redis, self.func_mgr)

            # Add to zones list if not already there
            try:
                self.redis.execute_command(['SADD', self.zones_list_key, zone_id])
            except Exception:
                pass  # Ignore if Redis operation fails

        return self.zones[zone_id]

    cpdef list get_all_zones(self):
        """Get list of all zones in this world"""
        try:
            result = self.redis.execute_command(['SMEMBERS', self.zones_list_key])
            return result if isinstance(result, list) else []
        except Exception:
            return []

    cpdef dict process_cross_zone_transfers(self):
        """Process pending cross-zone transfers"""
        xfer_stream = f"cy:xmsg:{{{self.world_id}}}"
        processed = 0

        try:
            # Read pending transfers
            transfers = self.redis.execute_command(['XREAD', 'COUNT', '100',
                                                   'STREAMS', xfer_stream, '0'])

            if transfers and len(transfers) > 0:
                for entry in transfers[0][1]:
                    entry_id = entry[0]
                    fields = entry[1]

                    # Extract transfer data
                    transfer_type = None
                    blob = None
                    for i in range(0, len(fields), 2):
                        if fields[i] == 'type':
                            transfer_type = fields[i + 1]
                        elif fields[i] == 'blob':
                            blob = fields[i + 1]

                    if transfer_type == 'xfer' and blob:
                        # Apply transfer to target zone
                        # Parse blob to get target zone
                        import json
                        try:
                            transfer_data = json.loads(blob)
                            target_zone = transfer_data.get('toZ')
                            if target_zone:
                                zone = self.get_zone(target_zone)
                                zone_events = zone.events_stream
                                zone_spatial = zone.spatial_index

                                # Apply the transfer with world context
                                self.func_mgr.call_function("cy:game", "xfer.apply",
                                                          [zone_events, zone_spatial],
                                                          [blob, self.world_id])
                                processed += 1
                        except (json.JSONDecodeError, KeyError):
                            pass  # Skip malformed transfers

                    # Acknowledge processing
                    self.redis.execute_command(['XDEL', xfer_stream, entry_id])

        except Exception:
            pass  # Continue even if processing fails

        return {'processed': processed}

# Game Engine main class
cdef class CyGameEngine:
    """
    Main game engine coordinating worlds and zones
    """

    cdef CyRedisClient redis
    cdef CyRedisFunctionsManager func_mgr
    cdef dict worlds
    cdef ThreadPoolExecutor executor

    def __cinit__(self, CyRedisClient redis_client):
        self.redis = redis_client
        self.func_mgr = CyRedisFunctionsManager(redis_client)
        self.worlds = {}
        self.executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef void load_game_functions(self):
        """Load the game engine Redis Functions"""
        try:
            self.func_mgr.load_library("cy:game", "1.0.0")
            print("✓ Game engine functions loaded")
        except Exception as e:
            print(f"✗ Failed to load game functions: {e}")
            # Try to create the functions directly
            try:
                self.redis.execute_command(['FUNCTION', 'LOAD', 'REPLACE', GAME_ENGINE_FUNCTIONS])
                print("✓ Game functions created directly")
            except Exception as e2:
                print(f"✗ Failed to create functions: {e2}")

    cpdef CyGameWorld get_world(self, str world_id):
        """Get or create a game world"""
        if world_id not in self.worlds:
            self.worlds[world_id] = CyGameWorld(world_id, self.redis, self.func_mgr)
        return self.worlds[world_id]

    cpdef dict tick_zone(self, str world_id, str zone_id, long dt_ms=DEFAULT_TICK_MS,
                        int budget=MAX_INTENTS_PER_TICK):
        """Tick a specific zone"""
        world = self.get_world(world_id)
        zone = world.get_zone(zone_id)

        now_ms = int(time.time() * 1000)

        # Check if tick is due
        if zone.is_tick_due(now_ms, dt_ms):
            return zone.step_tick(now_ms, dt_ms, budget)
        else:
            return {'tick': 'not_due', 'intents_consumed': 0}

    cpdef dict get_engine_stats(self):
        """Get engine-wide statistics"""
        stats = {
            'worlds': len(self.worlds),
            'total_zones': 0,
            'functions_loaded': len(self.func_mgr.loaded_scripts)
        }

        for world in self.worlds.values():
            zones = world.get_all_zones()
            stats['total_zones'] += len(zones)

        return stats

# Python wrapper
class GameEngine:
    """
    CyRedis Game Engine - Authoritative ECS on Redis

    Features:
    - Distributed, server-side Entity-Component-System
    - Horizontal scaling across Redis Cluster nodes
    - Authoritative simulation with discrete ticks
    - Atomic operations via Redis Functions
    - Cross-zone entity transfers
    - Real-time event streaming
    """

    def __init__(self, redis_client=None):
        if redis_client is None:
            from optimized_redis import OptimizedRedis
            redis_client = OptimizedRedis()

        self._engine = CyGameEngine(redis_client.client)
        self.redis = redis_client

    def load_functions(self):
        """Load game engine Redis Functions"""
        self._engine.load_game_functions()

    def get_world(self, world_id: str):
        """Get a game world"""
        return self._engine.get_world(world_id)

    def tick_zone(self, world_id: str, zone_id: str, dt_ms: int = DEFAULT_TICK_MS,
                  budget: int = MAX_INTENTS_PER_TICK):
        """Execute one tick for a zone"""
        return self._engine.tick_zone(world_id, zone_id, dt_ms, budget)

    async def tick_zone_async(self, world_id: str, zone_id: str,
                             dt_ms: int = DEFAULT_TICK_MS,
                             budget: int = MAX_INTENTS_PER_TICK):
        """Async version of tick_zone"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.tick_zone, world_id, zone_id, dt_ms, budget
        )

    def get_stats(self):
        """Get engine statistics"""
        return self._engine.get_engine_stats()

# Convenience functions
def create_game_engine(redis_client=None):
    """Create a game engine instance"""
    return GameEngine(redis_client)

def run_zone_worker(engine: GameEngine, world_id: str, zone_id: str,
                   tick_ms: int = DEFAULT_TICK_MS):
    """Run a worker that continuously ticks a zone"""
    import time

    print(f"🎮 Starting zone worker for {world_id}:{zone_id}")

    while True:
        try:
            result = engine.tick_zone(world_id, zone_id, tick_ms)

            if result.get('tick') != 'not_due':
                tick_num = result.get('tick', 0)
                consumed = result.get('intents_consumed', 0)
                if consumed > 0:
                    print(f"🎯 Zone {zone_id}: Tick {tick_num}, processed {consumed} intents")

            # Sleep for a fraction of tick time
            time.sleep(tick_ms / 1000.0 / 4)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Zone worker error: {e}")
            time.sleep(1.0)

    print(f"🛑 Zone worker for {world_id}:{zone_id} stopped")
