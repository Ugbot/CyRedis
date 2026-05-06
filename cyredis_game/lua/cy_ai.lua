#!lua name=cy_ai

-- cy_ai: AI control library using C-backed pathfinding and GOAP planner.
-- Users can FUNCTION LOAD their own cy_ai override to customise AI behaviour.

-- ── Pathfinding ───────────────────────────────────────────────────────────────

-- Find a path on a grid stored in Redis.
-- keys[1]=grid_key; args[1]=sx args[2]=sy args[3]=gx args[4]=gy args[5]=max_steps
redis.register_function('ai_find_path', function(keys, args)
    local grid = keys[1]
    local sx, sy = args[1], args[2]
    local gx, gy = args[3], args[4]
    local max_steps = args[5] or '1024'
    if not (sx and sy and gx and gy) then return {} end
    return redis.call('CYPATH.FIND', grid, sx, sy, gx, gy, max_steps)
end)

-- Tactical move: pathfind but prune waypoints that exceed an influence threshold.
-- keys[1]=grid_key keys[2]=influence_key
-- args[1]=sx args[2]=sy args[3]=gx args[4]=gy args[5]=threat_threshold
redis.register_function('ai_tactical_move', function(keys, args)
    local grid_key = keys[1]
    local inf_key  = keys[2]
    local sx, sy = args[1], args[2]
    local gx, gy = args[3], args[4]
    local threshold = tonumber(args[5] or '5')

    local path = redis.call('CYPATH.FIND', grid_key, sx, sy, gx, gy, '1024')
    if not path or #path == 0 then return {} end

    -- Walk path, stop before any cell whose influence exceeds threshold.
    local safe = {}
    for i = 1, #path, 2 do
        local px, py = path[i], path[i+1]
        local inf_val = 0
        if inf_key and #inf_key > 0 then
            local raw = redis.call('HGET', inf_key, px .. ':' .. py)
            inf_val = tonumber(raw or '0')
        end
        if inf_val > threshold then break end
        table.insert(safe, px)
        table.insert(safe, py)
    end
    return safe
end)

-- ── GOAP Planning ─────────────────────────────────────────────────────────────

-- Plan a sequence of actions to satisfy a goal.
-- keys[1]=worldstate_key keys[2]=actions_key
-- args[1]=goal_fact args[2]=goal_value args[3]=max_depth (default 10)
redis.register_function('ai_goap_plan', function(keys, args)
    local ws_key      = keys[1]
    local actions_key = keys[2]
    local goal_fact   = args[1]
    local goal_val    = args[2]
    local max_depth   = args[3] or '10'
    if not (goal_fact and goal_val) then return {} end
    return redis.call('CYGOAP.PLAN', ws_key, actions_key, goal_fact, goal_val, max_depth)
end)

-- Apply the first action from a plan to the world state.
-- keys[1]=worldstate_key keys[2]=actions_key; args[1]=action_name
redis.register_function('ai_goap_apply', function(keys, args)
    local ws_key      = keys[1]
    local actions_key = keys[2]
    local action      = args[1]
    if not action then return {'failed', 'no_action'} end
    return redis.call('CYGOAP.APPLY', ws_key, actions_key, action)
end)

-- Full plan-and-execute loop: plan up to max_depth actions, apply them all.
-- keys[1]=worldstate_key keys[2]=actions_key
-- args[1]=goal_fact args[2]=goal_value args[3]=max_depth
redis.register_function('ai_execute_plan', function(keys, args)
    local ws_key      = keys[1]
    local actions_key = keys[2]
    local goal_fact   = args[1]
    local goal_val    = args[2]
    local max_depth   = tonumber(args[3] or '10')

    local plan = redis.call('CYGOAP.PLAN', ws_key, actions_key, goal_fact, goal_val, tostring(max_depth))
    if not plan or #plan == 0 then return {'no_plan'} end

    local applied = {}
    for _, action in ipairs(plan) do
        local result = redis.call('CYGOAP.APPLY', ws_key, actions_key, action)
        table.insert(applied, action)
        -- Stop if apply failed
        if result and result[1] == 'failed' then
            table.insert(applied, 'FAILED:' .. (result[2] or '?'))
            break
        end
    end
    return applied
end)

-- ── FLECS Query helpers ───────────────────────────────────────────────────────

-- Query entities by component filter in a world.
-- keys[1]=world_id; args[1]=filter_string args[2]=zone_id (optional)
redis.register_function('ai_query', function(keys, args)
    local world  = keys[1]
    local filter = args[1] or 'Position'
    local zone   = args[2]
    if zone and #zone > 0 then
        return redis.call('FLECS.QUERY', world, filter, zone)
    end
    return redis.call('FLECS.QUERY', world, filter)
end)

-- Spatial proximity query using physics circle sweep.
-- keys[1]=spatial_zset_key; args[1]=cx args[2]=cy args[3]=radius args[4]=limit
redis.register_function('ai_nearby', function(keys, args)
    local spatial_key = keys[1]
    local cx, cy      = args[1], args[2]
    local radius      = args[3] or '10'
    local limit       = args[4] or '50'
    if not (cx and cy) then return {} end
    return redis.call('CYPHYS.CIRCLE', spatial_key, cx, cy, radius, limit)
end)

-- ── Composite behaviours ──────────────────────────────────────────────────────

-- Pursue nearest enemy: find nearby entities, pathfind toward closest.
-- keys[1]=world_id keys[2]=grid_key keys[3]=spatial_key
-- args[1]=agent_eid args[2]=zone args[3]=enemy_type (unused, reserved)
redis.register_function('ai_pursue_nearest', function(keys, args)
    local world       = keys[1]
    local grid_key    = keys[2]
    local spatial_key = keys[3]
    local agent_eid   = args[1]
    local zone        = args[2]

    -- Get agent position
    local pos = redis.call('FLECS.GETCOMP', world, agent_eid, 'Position')
    if not pos or #pos == 0 then return {0, 'agent_missing'} end
    local ax, ay = nil, nil
    for i = 1, #pos - 1, 2 do
        if pos[i] == 'x' then ax = pos[i+1]
        elseif pos[i] == 'y' then ay = pos[i+1] end
    end
    if not (ax and ay) then return {0, 'no_position'} end

    -- Find nearby entities
    local nearby = redis.call('CYPHYS.CIRCLE', spatial_key, ax, ay, '20', '5')
    if not nearby or #nearby == 0 then return {0, 'no_targets'} end

    -- Pick first target (closest, already sorted by CYPHYS.CIRCLE)
    local target_eid = nearby[1]
    if target_eid == agent_eid and #nearby >= 3 then
        target_eid = nearby[3]  -- skip self
    end

    -- Get target position
    local tpos = redis.call('FLECS.GETCOMP', world, target_eid, 'Position')
    if not tpos or #tpos == 0 then return {0, 'target_missing'} end
    local tx, ty = nil, nil
    for i = 1, #tpos - 1, 2 do
        if tpos[i] == 'x' then tx = tpos[i+1]
        elseif tpos[i] == 'y' then ty = tpos[i+1] end
    end
    if not (tx and ty) then return {0, 'target_no_position'} end

    -- Pathfind toward target
    local path = redis.call('CYPATH.FIND', grid_key,
        tostring(math.floor(tonumber(ax))),
        tostring(math.floor(tonumber(ay))),
        tostring(math.floor(tonumber(tx))),
        tostring(math.floor(tonumber(ty))),
        '64')

    return {1, target_eid, table.unpack(path or {})}
end)
