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
from cy_redis.core.cy_redis_client import CyRedisClient
from cy_redis.features.functions import CyRedisFunctionsManager

# Compile-time constants (used as default parameter values in cdef/cpdef)
DEF _TICK_MS = 50
DEF _MAX_INTENTS = 256
DEF _SPATIAL_PREC = 1000

# Python-visible module constants
DEFAULT_TICK_MS = 50
MAX_INTENTS_PER_TICK = 256
SPATIAL_PRECISION = 1000

# Game Engine Redis Functions (Lua code)
GAME_ENGINE_FUNCTIONS = """#!lua name=cy_game
-- cy:game - Redis Game Engine Functions

-- Tick helpers
redis.register_function('tick_fetch_due', function(keys, args)
    local k, now, tick = keys[1], tonumber(args[1]), tonumber(args[2])
    local t = tonumber(redis.call('HGET', k, 't') or 0)
    local last = tonumber(redis.call('HGET', k, 'last_ms') or 0)
    if now - last >= tick then
        return {1, t+1}
    else
        return {0, t}
    end
end)

redis.register_function('tick_step', function(keys, args)
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
                -- apply velocity for dt to position
                local keyEnt = 'cy:ent:{'..W..':'..Z..'}:'..eid
                local ent = redis.call('HMGET', keyEnt, 'x','y','vx','vy','type','version')
                if ent and ent[1] then
                    local x = tonumber(ent[1] or '0') + tonumber(ent[3] or '0') * dt/1000.0
                    local y = tonumber(ent[2] or '0') + tonumber(ent[4] or '0') * dt/1000.0
                    local ver = (tonumber(ent[6] or '0') + 1)
                    redis.call('HSET', keyEnt, 'x', x, 'y', y, 'version', ver)
                    -- update spatial index: encode as x_int * 1e9 + y_int for range queries
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

    -- 2) advance tick clock
    local t = tonumber(redis.call('HINCRBY', kTick, 't', 1))
    redis.call('HSET', kTick, 'last_ms', now)
    return {t, consumed}
end)

-- Entity operations
redis.register_function('ent_spawn', function(keys, args)
    local kEnt, kSp, kType = keys[1], keys[2], keys[3]
    local eid, typ = args[1], args[2]
    local x, y = tonumber(args[3]), tonumber(args[4])
    local vx, vy = tonumber(args[5] or '0'), tonumber(args[6] or '0')
    if redis.call('EXISTS', kEnt)==1 then return {0, 'exists'} end
    redis.call('HSET', kEnt, 'eid', eid, 'type', typ, 'x', x, 'y', y, 'vx', vx, 'vy', vy, 'hp', 100, 'version', 1)
    local score = math.floor(x*1000)*1000000000 + math.floor(y*1000)
    redis.call('ZADD', kSp, score, eid)
    redis.call('SADD', kType, eid)
    return {1, 'ok'}
end)

redis.register_function('ent_apply_damage', function(keys, args)
    local kEnt, kEv, kType = keys[1], keys[2], keys[3]
    local dmg = tonumber(args[1])
    local hp = tonumber(redis.call('HGET', kEnt, 'hp') or '0') - dmg
    if hp <= 0 then
        local eid = redis.call('HGET', kEnt, 'eid')
        local typ = redis.call('HGET', kEnt, 'type')
        redis.call('DEL', kEnt)
        if eid and typ then redis.call('SREM', kType, eid) end
        redis.call('XADD', kEv, '*', 'eid', eid or '', 'type', 'death')
        return {1, 'dead'}
    else
        redis.call('HSET', kEnt, 'hp', hp)
        return {1, 'hp:'..hp}
    end
end)

-- Scheduling
redis.register_function('sched_enqueue', function(keys, args)
    local kSched, id, run_at, payload = keys[1], args[1], tonumber(args[2]), args[3]
    redis.call('ZADD', kSched, run_at, id)
    redis.call('HSET', kSched..':payload', id, payload)
    return 1
end)

redis.register_function('sched_due', function(keys, args)
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

-- Cross-zone transfer: serialize entity to pipe-delimited blob and queue
redis.register_function('xfer_request', function(keys, args)
    local kEnt, kOut, toZ = keys[1], keys[2], args[1]
    local all = redis.call('HGETALL', kEnt)
    if #all==0 then return {0, 'missing'} end
    local eid = redis.call('HGET', kEnt, 'eid') or 'unknown'
    local typ = redis.call('HGET', kEnt, 'type') or 'unknown'
    local x   = redis.call('HGET', kEnt, 'x') or '0'
    local y   = redis.call('HGET', kEnt, 'y') or '0'
    local vx  = redis.call('HGET', kEnt, 'vx') or '0'
    local vy  = redis.call('HGET', kEnt, 'vy') or '0'
    local ver = redis.call('HGET', kEnt, 'version') or '1'
    local hp  = redis.call('HGET', kEnt, 'hp') or '100'
    -- blob format: eid|type|x|y|vx|vy|version|hp|toZ
    local blob = eid..'|'..typ..'|'..x..'|'..y..'|'..vx..'|'..vy..'|'..ver..'|'..hp..'|'..toZ
    redis.call('XADD', kOut, '*', 'type', 'xfer', 'blob', blob)
    redis.call('DEL', kEnt)
    return {1, 'queued'}
end)

redis.register_function('xfer_apply', function(keys, args)
    local kEv, kSp, kType = keys[1], keys[2], keys[3]
    local blob, W = args[1], args[2]
    -- parse blob: eid|type|x|y|vx|vy|version|hp|toZ
    local parts = {}
    for part in string.gmatch(blob, '([^|]+)') do
        table.insert(parts, part)
    end
    if #parts < 9 then return {0, 'invalid_format'} end
    local eid, typ, x, y, vx, vy, ver, hp, toZ = parts[1],parts[2],parts[3],parts[4],parts[5],parts[6],parts[7],parts[8],parts[9]
    local kEnt = 'cy:ent:{'..W..':'..toZ..'}:'..eid
    redis.call('HSET', kEnt, 'eid', eid, 'type', typ, 'x', x, 'y', y,
               'vx', vx, 'vy', vy, 'version', ver, 'hp', hp)
    local score = math.floor(tonumber(x)*1000)*1000000000 + math.floor(tonumber(y)*1000)
    redis.call('ZADD', kSp, score, eid)
    redis.call('SADD', kType, typ..':'..eid)
    redis.call('XADD', kEv, '*', 'eid', eid, 'type', 'spawn', 'zone', toZ)
    return {1, 'applied'}
end)
"""

# World and Zone management
cdef class CyZone:
    """
    Represents a game zone with its own simulation space
    """

    def __cinit__(self, str world_id, str zone_id, object redis, object func_mgr):
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

        # Subsystems initialised lazily
        self._pathfinder = None
        self._physics    = None
        self._module_mgr = None

    def get_entity_key(self, str entity_id):
        """Get the full entity key for this zone"""
        return f"cy:ent:{{{self.world_id}:{self.zone_id}}}:{entity_id}"

    def get_type_set_key(self, str entity_type):
        """Get the type index key"""
        return f"cy:type:{{{self.world_id}:{self.zone_id}}}:{entity_type}"

    cpdef bint is_tick_due(self, long now_ms, long tick_ms=_TICK_MS):
        """Check if zone tick is due (non-mutating peek)"""
        try:
            result = self.func_mgr.call_function("tick_fetch_due",
                                                 [self.tick_key],
                                                 [str(now_ms), str(tick_ms)])
            return bool(result and result[0] == 1)
        except Exception:
            return False

    cpdef dict step_tick(self, long now_ms, long dt_ms=_TICK_MS,
                        int budget=_MAX_INTENTS):
        """Execute one tick step: drain intents, run systems, advance clock"""
        try:
            result = self.func_mgr.call_function("tick_step",
                                                 [self.tick_key, self.intents_stream,
                                                  self.events_stream, self.spatial_index],
                                                 [str(now_ms), str(dt_ms), str(budget),
                                                  self.world_id, self.zone_id])
            if result and len(result) >= 2:
                return {'tick': result[0], 'intents_consumed': result[1]}
        except Exception:
            pass
        return {'tick': 0, 'intents_consumed': 0}

    cpdef dict spawn_entity(self, str entity_id, str entity_type,
                           double x, double y, double vx=0.0, double vy=0.0):
        """Spawn a new entity in this zone"""
        ent_key = self.get_entity_key(entity_id)
        type_key = self.get_type_set_key(entity_type)

        try:
            result = self.func_mgr.call_function("ent_spawn",
                                                 [ent_key, self.spatial_index, type_key],
                                                 [entity_id, entity_type,
                                                  str(x), str(y), str(vx), str(vy)])
            if result and result[0] == 1:
                return {'success': True, 'status': result[1]}
            return {'success': False, 'status': result[1] if result else 'unknown'}
        except Exception as e:
            return {'success': False, 'status': str(e)}

    cpdef dict apply_damage(self, str entity_id, int damage):
        """Apply damage to an entity; returns status 'hp:N' or 'dead'"""
        ent_key = self.get_entity_key(entity_id)

        # Look up the entity type so we can clean up the type set
        try:
            entity_type = self.redis.execute_command(['HGET', ent_key, 'type'])
        except Exception:
            entity_type = None

        type_key = self.get_type_set_key(entity_type or "unknown")

        try:
            result = self.func_mgr.call_function("ent_apply_damage",
                                                 [ent_key, self.events_stream, type_key],
                                                 [str(damage)])
            if result and result[0] == 1:
                return {'success': True, 'status': result[1]}
            return {'success': False, 'status': 'entity_not_found'}
        except Exception as e:
            return {'success': False, 'status': str(e)}

    cpdef bint send_intent(self, str entity_id, str intent_type, payload=None):
        """Send an intent to this zone using fast binary serialization"""
        if payload is None:
            payload = {}

        now_ms = int(time.time() * 1000)

        try:
            payload_bytes = serialize_game_data(payload)
            payload_hex = payload_bytes.hex()  # hex encoding for Redis compatibility

            self.redis.execute_command(['XADD', self.intents_stream, '*',
                                       'eid', entity_id,
                                       'kind', intent_type,
                                       'payload', payload_hex,
                                       'ts', str(now_ms)])
            return True
        except Exception:
            return False

    cpdef list read_events(self, str last_id="0", int count=100):
        """Read events from this zone with fast binary deserialization"""
        try:
            result = self.redis.execute_command(['XREAD', 'COUNT', str(count),
                                                'STREAMS', self.events_stream, last_id])
            if not result or len(result) == 0:
                return []

            events = result[0][1]  # stream entries for first (and only) stream
            deserialized_events = []
            for entry in events:
                event_id = entry[0]
                raw_fields = entry[1]

                # XREAD returns fields as a flat list [key, val, key, val, ...]
                event_data = {}
                if isinstance(raw_fields, list):
                    for i in range(0, len(raw_fields) - 1, 2):
                        k = raw_fields[i]
                        v = raw_fields[i + 1]
                        if k == 'payload' and v:
                            try:
                                event_data[k] = deserialize_game_data(bytes.fromhex(v))
                            except Exception:
                                event_data[k] = v
                        else:
                            event_data[k] = v
                elif isinstance(raw_fields, dict):
                    # Some Redis client versions return dicts
                    for k, v in raw_fields.items():
                        if k == 'payload' and v:
                            try:
                                event_data[k] = deserialize_game_data(bytes.fromhex(v))
                            except Exception:
                                event_data[k] = v
                        else:
                            event_data[k] = v

                deserialized_events.append((event_id, event_data))
            return deserialized_events
        except Exception:
            return []

    cpdef dict schedule_job(self, str job_id, long run_at_ms, str payload=""):
        """Schedule a job for future execution"""
        try:
            result = self.func_mgr.call_function("sched_enqueue",
                                                 [self.schedule_zset],
                                                 [job_id, str(run_at_ms), payload])
            return {'success': result == 1}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    cpdef list get_due_jobs(self, long now_ms, int limit=100):
        """Get and remove jobs that are due for execution"""
        try:
            result = self.func_mgr.call_function("sched_due",
                                                 [self.schedule_zset],
                                                 [str(now_ms), str(limit)])
            jobs = []
            if result:
                for i in range(0, len(result) - 1, 2):
                    jobs.append({'job_id': result[i], 'payload': result[i + 1]})
            return jobs
        except Exception:
            return []

    cpdef dict transfer_entity(self, str entity_id, str target_zone):
        """Initiate entity transfer to another zone via the cross-zone stream"""
        ent_key = self.get_entity_key(entity_id)
        xfer_stream = f"cy:xmsg:{{{self.world_id}}}"

        try:
            result = self.func_mgr.call_function("xfer_request",
                                                 [ent_key, xfer_stream],
                                                 [target_zone])
            if result and result[0] == 1:
                return {'success': True, 'status': result[1]}
            return {'success': False, 'status': result[1] if result else 'unknown'}
        except Exception as e:
            return {'success': False, 'status': str(e)}

    cpdef list query_spatial(self, double x_min, double x_max,
                             double y_min, double y_max, int limit=100):
        """Query entities in a rectangular region using the spatial ZSET index.

        Score encoding: floor(x*1000)*1e9 + floor(y*1000)
        We scan the full score range and filter in Python — good enough for typical
        zone sizes.  For tighter areas, callers can pass a tighter limit.
        """
        cdef long score_min = <long>(x_min * 1000) * 1000000000 + <long>(y_min * 1000)
        cdef long score_max = <long>(x_max * 1000) * 1000000000 + <long>(y_max * 1000)
        try:
            result = self.redis.execute_command([
                'ZRANGEBYSCORE', self.spatial_index,
                str(score_min), str(score_max),
                'LIMIT', '0', str(limit)
            ])
            return result if result else []
        except Exception:
            return []

    # ── Key helpers ────────────────────────────────────────────────────────

    cpdef str get_nav_grid_key(self):
        return f"cy:nav:{{{self.world_id}:{self.zone_id}}}:grid"

    cpdef str get_ws_key(self, str agent_id):
        return f"cy:ws:{{{self.world_id}:{self.zone_id}}}:{agent_id}"

    cpdef str get_actions_key(self):
        return f"cy:actions:{{{self.world_id}:{self.zone_id}}}"

    # ── Subsystem delegation — CYPATH (A*) ─────────────────────────────────

    cpdef list find_path(self, int sx, int sy, int gx, int gy, int max_steps=1024):
        """Find a path on the zone's nav grid using the CYPATH C module.

        Returns a list of (x, y) tuples or [] if no path exists.
        Requires the cy_game Redis module to be loaded.
        """
        try:
            result = self.redis.execute_command([
                "CYPATH.FIND", self.get_nav_grid_key(),
                str(sx), str(sy), str(gx), str(gy), str(max_steps),
            ])
        except Exception:
            return []
        if not result:
            return []
        path = []
        cdef int i
        for i in range(0, len(result) - 1, 2):
            try:
                path.append((int(result[i]), int(result[i + 1])))
            except (ValueError, TypeError):
                pass
        return path

    # ── Subsystem delegation — CYPHYS (circle query) ───────────────────────

    cpdef list physics_circle_query(self, double cx, double cy, double radius, int limit=50):
        """Query entities within radius of (cx, cy) using the spatial ZSET.

        Returns a list of (entity_id, distance_str) tuples sorted by distance.
        Requires the cy_game Redis module to be loaded.
        """
        try:
            raw = self.redis.execute_command([
                "CYPHYS.CIRCLE", self.spatial_index,
                str(cx), str(cy), str(radius), str(limit),
            ])
        except Exception:
            return []
        if not raw:
            return []
        results = []
        cdef int j
        for j in range(0, len(raw) - 1, 2):
            results.append((raw[j], raw[j + 1]))
        return results

    # ── Subsystem delegation — FLECS ECS ───────────────────────────────────

    cpdef list flecs_query(self, str filter_string):
        """Query entity IDs from the FLECS world matching a component filter.

        filter_string is a comma-separated list of component names, e.g.
        ``"Position, Health"``.  Returns a list of entity ID strings.
        Requires the cy_game Redis module + FLECS.INIT for this world.
        """
        try:
            result = self.redis.execute_command([
                "FLECS.QUERY", self.world_id, filter_string, self.zone_id,
            ])
            return list(result) if result else []
        except Exception:
            return []

    cpdef dict flecs_spawn(self, str entity_id, str entity_type,
                           double x, double y, double vx=0.0, double vy=0.0, int hp=100):
        """Spawn entity in both FLECS world and Redis persistence layer.

        Delegates to FLECS.SPAWN which handles both at once.
        Falls back to the Lua-backed spawn if the module is not loaded.
        """
        try:
            result = self.redis.execute_command([
                "FLECS.SPAWN", self.world_id, self.zone_id,
                entity_id, entity_type,
                str(x), str(y), str(vx), str(vy), str(hp),
            ])
            if result == 1:
                return {'success': True, 'status': 'ok', 'backend': 'flecs'}
            return {'success': False, 'status': 'duplicate', 'backend': 'flecs'}
        except Exception:
            # Module not loaded — fall back to Lua-backed spawn
            return self.spawn_entity(entity_id, entity_type, x, y, vx, vy)

    cpdef dict flecs_tick(self, long dt_ms=_TICK_MS, int budget=_MAX_INTENTS):
        """Run FLECS.TICK for this zone.

        Returns {'tick': N, 'entities': N, 'intents': N} or falls back to
        the Lua-backed step_tick if the module is not loaded.
        """
        import time as _time
        now_ms = int(_time.time() * 1000)
        try:
            raw = self.redis.execute_command([
                "FLECS.TICK", self.world_id, self.zone_id,
                str(dt_ms), str(budget),
            ])
            if raw and len(raw) >= 6:
                return {
                    'tick':     raw[1] if len(raw) > 1 else 0,
                    'entities': raw[3] if len(raw) > 3 else 0,
                    'intents':  raw[5] if len(raw) > 5 else 0,
                    'backend':  'flecs',
                }
            return {'tick': 0, 'entities': 0, 'intents': 0, 'backend': 'flecs'}
        except Exception:
            result = self.step_tick(now_ms, dt_ms, budget)
            result['backend'] = 'lua'
            return result


# World manager
cdef class CyGameWorld:
    """
    Manages a game world with multiple zones
    """

    def __cinit__(self, str world_id, object redis, object func_mgr):
        self.world_id = world_id
        self.redis = redis
        self.func_mgr = func_mgr
        self.zones = {}
        self.zones_list_key = f"cy:world:{world_id}:zones"

    cpdef CyZone get_zone(self, str zone_id):
        """Get or create a zone"""
        if zone_id not in self.zones:
            self.zones[zone_id] = CyZone(self.world_id, zone_id, self.redis, self.func_mgr)
            try:
                self.redis.execute_command(['SADD', self.zones_list_key, zone_id])
            except Exception:
                pass
        return self.zones[zone_id]

    cpdef list get_all_zones(self):
        """Get list of all zones in this world"""
        try:
            result = self.redis.execute_command(['SMEMBERS', self.zones_list_key])
            return result if isinstance(result, list) else []
        except Exception:
            return []

    cpdef dict process_cross_zone_transfers(self):
        """Process pending cross-zone transfers from the xmsg stream"""
        xfer_stream = f"cy:xmsg:{{{self.world_id}}}"
        processed = 0

        try:
            transfers = self.redis.execute_command(['XREAD', 'COUNT', '100',
                                                   'STREAMS', xfer_stream, '0'])
            if not transfers or len(transfers) == 0:
                return {'processed': 0}

            for entry in transfers[0][1]:
                entry_id = entry[0]
                raw_fields = entry[1]

                # Normalise flat list → dict
                if isinstance(raw_fields, list):
                    fields = {}
                    for i in range(0, len(raw_fields) - 1, 2):
                        fields[raw_fields[i]] = raw_fields[i + 1]
                else:
                    fields = raw_fields

                transfer_type = fields.get('type')
                blob = fields.get('blob')

                if transfer_type == 'xfer' and blob:
                    # blob format: eid|type|x|y|vx|vy|version|hp|toZ
                    parts = blob.split('|')
                    if len(parts) >= 9:
                        target_zone = parts[8]
                        zone = self.get_zone(target_zone)

                        self.func_mgr.call_function("xfer_apply",
                                                    [zone.events_stream,
                                                     zone.spatial_index,
                                                     zone.get_type_set_key(parts[1])],
                                                    [blob, self.world_id])
                        processed += 1

                # Remove processed entry
                try:
                    self.redis.execute_command(['XDEL', xfer_stream, entry_id])
                except Exception:
                    pass

        except Exception:
            pass

        return {'processed': processed}


# Game Engine main class
cdef class CyGameEngine:
    """
    Main game engine coordinating worlds and zones
    """

    def __cinit__(self, object redis_client):
        self.redis = redis_client
        self.func_mgr = CyRedisFunctionsManager(redis_client)
        self.worlds = {}
        self.executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef void load_game_functions(self):
        """Load the game engine Redis Functions directly via FUNCTION LOAD"""
        try:
            self.redis.execute_command(['FUNCTION', 'LOAD', 'REPLACE', GAME_ENGINE_FUNCTIONS])
        except Exception as e:
            raise RuntimeError(f"Failed to load game engine functions: {e}")

    cpdef CyGameWorld get_world(self, str world_id):
        """Get or create a game world"""
        if world_id not in self.worlds:
            self.worlds[world_id] = CyGameWorld(world_id, self.redis, self.func_mgr)
        return self.worlds[world_id]

    cpdef dict tick_zone(self, str world_id, str zone_id, long dt_ms=_TICK_MS,
                        int budget=_MAX_INTENTS):
        """Tick a specific zone"""
        world = self.get_world(world_id)
        zone = world.get_zone(zone_id)
        now_ms = int(time.time() * 1000)

        if zone.is_tick_due(now_ms, dt_ms):
            return zone.step_tick(now_ms, dt_ms, budget)
        return {'tick': 'not_due', 'intents_consumed': 0}

    cpdef dict get_engine_stats(self):
        """Get engine-wide statistics"""
        cdef int total_zones = 0
        for world in self.worlds.values():
            total_zones += len(world.get_all_zones())
        return {
            'worlds': len(self.worlds),
            'total_zones': total_zones,
            'libraries_loaded': len(self.func_mgr.list_loaded_libraries()),
        }


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
            redis_client = CyRedisClient()
        elif hasattr(redis_client, '_client'):
            # Accept wrapper objects that expose the underlying client
            redis_client = redis_client._client

        self._engine = CyGameEngine(redis_client)
        self.redis = redis_client

    def load_functions(self):
        """Load game engine Redis Functions"""
        self._engine.load_game_functions()

    def load_module(self, path: str = None):
        """Load cy_game.so into Redis.

        Defaults to cyredis_game/module/cy_game.so relative to the package.
        Idempotent — safe to call even if already loaded.
        """
        from cyredis_game.module_manager import CyGameModule
        mod = CyGameModule(self.redis)
        mod.load(path or "")

    def init_world(self, world_id: str):
        """Initialise a FLECS world (idempotent)."""
        self.redis.execute_command(["FLECS.INIT", world_id])

    def restore_world(self, world_id: str) -> int:
        """Restore FLECS world from Redis-persisted entity hashes.

        Returns the number of entities loaded.
        """
        result = self.redis.execute_command(["FLECS.RESTORE", world_id])
        try:
            return int(result)
        except Exception:
            return 0

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
    """Run a worker that continuously ticks a zone until interrupted"""
    while True:
        try:
            result = engine.tick_zone(world_id, zone_id, tick_ms)

            if result.get('tick') != 'not_due':
                consumed = result.get('intents_consumed', 0)
                if consumed > 0:
                    print(f"Zone {zone_id}: tick {result.get('tick', 0)}, "
                          f"{consumed} intents processed")

            # Poll at 4x the tick rate
            time.sleep(tick_ms / 1000.0 / 4)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Zone worker error ({world_id}:{zone_id}): {e}")
            time.sleep(1.0)
