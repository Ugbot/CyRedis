#!lua name=cy_game_flecs

-- cy_game_flecs: Gameplay orchestration library backed by the cy_game Redis module.
-- All simulation compute runs in C (FLECS ECS + pathfinding + physics + GOAP).
-- These Lua functions are user-configurable orchestration templates.

-- ── Tick ─────────────────────────────────────────────────────────────────────

-- Tick a zone: delegates entirely to FLECS.TICK (C speed, one call).
-- keys[1] = world_id
-- args[1] = zone_id, args[2] = dt_ms, args[3] = budget (default 256)
redis.register_function('flecs_tick', function(keys, args)
    local world = keys[1]
    local zone  = args[1]
    local dt    = args[2] or '50'
    local budget = args[3] or '256'
    return redis.call('FLECS.TICK', world, zone, dt, budget)
end)

-- ── Entity lifecycle ──────────────────────────────────────────────────────────

-- Spawn an entity into a FLECS world.
-- keys[1] = world_id
-- args[1..] = zone eid type x y [vx vy] [hp]
redis.register_function('flecs_spawn', function(keys, args)
    local world = keys[1]
    local cmd = {'FLECS.SPAWN', world}
    for _, a in ipairs(args) do table.insert(cmd, a) end
    return redis.call(table.unpack(cmd))
end)

-- Apply damage to an entity; kill if hp reaches 0.
-- keys[1] = world_id
-- args[1] = eid, args[2] = damage_amount
redis.register_function('flecs_apply_damage', function(keys, args)
    local world = keys[1]
    local eid   = args[1]
    local dmg   = tonumber(args[2])
    if not eid or not dmg then return {0, 'bad_args'} end

    local hp_data = redis.call('FLECS.GETCOMP', world, eid, 'Health')
    if not hp_data or #hp_data == 0 then return {0, 'not_found'} end

    -- hp_data is ['hp', <val>, 'max_hp', <val>]
    local hp = nil
    for i = 1, #hp_data - 1, 2 do
        if hp_data[i] == 'hp' then hp = tonumber(hp_data[i+1]) end
    end
    if not hp then return {0, 'no_hp'} end

    hp = hp - dmg
    if hp <= 0 then
        redis.call('FLECS.DELETE', world, eid)
        -- Emit death event
        local ev_key = 'cy:events:{' .. world .. ':*}'
        -- Best-effort; zone key not passed here so use world-level stream
        pcall(redis.call, 'XADD', 'cy:events:{' .. world .. '}', '*', 'eid', eid, 'type', 'death')
        return {1, 'dead'}
    end

    redis.call('FLECS.SETCOMP', world, eid, 'Health', 'hp', tostring(hp))
    return {1, 'hp:' .. hp}
end)

-- Teleport an entity to a new position (direct component set, no physics).
-- keys[1] = world_id; args[1]=eid args[2]=x args[3]=y
redis.register_function('flecs_teleport', function(keys, args)
    local world, eid, x, y = keys[1], args[1], args[2], args[3]
    if not (eid and x and y) then return {0, 'bad_args'} end
    if redis.call('FLECS.HAS', world, eid, 'Position') == 0 then
        return {0, 'not_found'}
    end
    redis.call('FLECS.SETCOMP', world, eid, 'Position', 'x', x, 'y', y)
    -- Update spatial ZSET (best-effort, zone unknown here)
    return {1, 'ok'}
end)

-- Set velocity on an entity.
-- keys[1]=world_id; args[1]=eid args[2]=vx args[3]=vy
redis.register_function('flecs_set_velocity', function(keys, args)
    local world, eid, vx, vy = keys[1], args[1], args[2], args[3]
    if not (eid and vx and vy) then return {0, 'bad_args'} end
    redis.call('FLECS.SETCOMP', world, eid, 'Velocity', 'vx', vx, 'vy', vy)
    return {1, 'ok'}
end)

-- ── Query helpers ─────────────────────────────────────────────────────────────

-- Query entities by component filter, optionally limited to a zone.
-- keys[1]=world_id; args[1]=filter_string args[2]=zone_id (optional)
redis.register_function('flecs_query', function(keys, args)
    local world  = keys[1]
    local filter = args[1] or 'Position'
    local zone   = args[2]
    if zone and #zone > 0 then
        return redis.call('FLECS.QUERY', world, filter, zone)
    end
    return redis.call('FLECS.QUERY', world, filter)
end)

-- Get full component snapshot of an entity (Position + Velocity + Health).
-- keys[1]=world_id; args[1]=eid
redis.register_function('flecs_snapshot', function(keys, args)
    local world, eid = keys[1], args[1]
    if not eid then return {} end
    local has_pos = redis.call('FLECS.HAS', world, eid, 'Position')
    local has_vel = redis.call('FLECS.HAS', world, eid, 'Velocity')
    local has_hp  = redis.call('FLECS.HAS', world, eid, 'Health')
    local out = {'eid', eid}
    if has_pos == 1 then
        local pos = redis.call('FLECS.GETCOMP', world, eid, 'Position')
        if pos then for _, v in ipairs(pos) do table.insert(out, v) end end
    end
    if has_vel == 1 then
        local vel = redis.call('FLECS.GETCOMP', world, eid, 'Velocity')
        if vel then for _, v in ipairs(vel) do table.insert(out, v) end end
    end
    if has_hp == 1 then
        local hp = redis.call('FLECS.GETCOMP', world, eid, 'Health')
        if hp then for _, v in ipairs(hp) do table.insert(out, v) end end
    end
    return out
end)

-- ── Zone batch operations ─────────────────────────────────────────────────────

-- Run N ticks at once, returning last tick stats.
-- keys[1]=world_id; args[1]=zone args[2]=dt_ms args[3]=n_ticks
redis.register_function('flecs_batch_tick', function(keys, args)
    local world  = keys[1]
    local zone   = args[1]
    local dt     = args[2] or '50'
    local n      = tonumber(args[3] or '1')
    local last   = nil
    for _ = 1, n do
        last = redis.call('FLECS.TICK', world, zone, dt, '256')
    end
    return last or {}
end)
