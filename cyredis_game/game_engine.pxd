# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Game Engine
"""

# Compile-time constants for default parameter values
DEF _TICK_MS = 50
DEF _MAX_INTENTS = 256
DEF _SPATIAL_PREC = 1000

# Zone class
cdef class CyZone:
    cdef str world_id
    cdef str zone_id
    cdef object redis
    cdef object func_mgr
    cdef readonly str tick_key
    cdef readonly str intents_stream
    cdef readonly str events_stream
    cdef readonly str spatial_index
    cdef readonly str schedule_zset

    # Subsystem objects (set lazily on first access)
    cdef object _pathfinder
    cdef object _physics
    cdef object _module_mgr

    cpdef bint is_tick_due(self, long now_ms, long tick_ms=*)
    cpdef dict step_tick(self, long now_ms, long dt_ms=*, int budget=*)
    cpdef dict spawn_entity(self, str entity_id, str entity_type,
                            double x, double y, double vx=*, double vy=*)
    cpdef dict apply_damage(self, str entity_id, int damage)
    cpdef bint send_intent(self, str entity_id, str intent_type, payload=*)
    cpdef list read_events(self, str last_id=*, int count=*)
    cpdef dict schedule_job(self, str job_id, long run_at_ms, str payload=*)
    cpdef list get_due_jobs(self, long now_ms, int limit=*)
    cpdef dict transfer_entity(self, str entity_id, str target_zone)
    cpdef list query_spatial(self, double x_min, double x_max,
                             double y_min, double y_max, int limit=*)

    # Subsystem delegation
    cpdef list find_path(self, int sx, int sy, int gx, int gy, int max_steps=*)
    cpdef list physics_circle_query(self, double cx, double cy, double radius, int limit=*)
    cpdef list flecs_query(self, str filter_string)
    cpdef dict flecs_spawn(self, str entity_id, str entity_type,
                           double x, double y, double vx=*, double vy=*, int hp=*)
    cpdef dict flecs_tick(self, long dt_ms=*, int budget=*)

    # Internal helpers
    cdef dict _decode_event_fields(self, object raw_fields)

    # Key helpers
    cpdef str get_nav_grid_key(self)
    cpdef str get_ws_key(self, str agent_id)
    cpdef str get_actions_key(self)

# World class
cdef class CyGameWorld:
    cdef str world_id
    cdef object redis
    cdef object func_mgr
    cdef dict zones
    cdef str zones_list_key

    cpdef CyZone get_zone(self, str zone_id)
    cpdef list get_all_zones(self)
    cpdef dict process_cross_zone_transfers(self)

# Game Engine
cdef class CyGameEngine:
    cdef object redis
    cdef readonly object func_mgr
    cdef dict worlds
    cdef object executor

    cpdef void load_game_functions(self)
    cpdef CyGameWorld get_world(self, str world_id)
    cpdef dict tick_zone(self, str world_id, str zone_id,
                         long dt_ms=*, int budget=*)
    cpdef dict get_engine_stats(self)
