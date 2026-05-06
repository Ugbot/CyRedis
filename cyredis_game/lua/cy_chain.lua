#!lua name=cy_chain

-- cy_chain: Multi-command atomic composition helpers.
-- Executes batches of game operations atomically within a single Lua call.

-- Execute a tick + collect nearby entities in one atomic call.
-- keys[1]=world_id keys[2]=spatial_key; args[1]=zone args[2]=dt_ms args[3]=cx args[4]=cy args[5]=radius
redis.register_function('chain_tick_and_query', function(keys, args)
    local world       = keys[1]
    local spatial_key = keys[2]
    local zone        = args[1]
    local dt          = args[2] or '50'
    local cx, cy      = args[3], args[4]
    local radius      = args[5] or '20'

    local tick_result = redis.call('FLECS.TICK', world, zone, dt, '256')
    local nearby      = {}
    if cx and cy then
        nearby = redis.call('CYPHYS.CIRCLE', spatial_key, cx, cy, radius, '50')
    end
    return {tick_result, nearby}
end)

-- Spawn + pathfind to target in one call.
-- keys[1]=world_id keys[2]=grid_key
-- args[1]=zone args[2]=eid args[3]=type args[4]=sx args[5]=sy args[6]=gx args[7]=gy
redis.register_function('chain_spawn_and_path', function(keys, args)
    local world    = keys[1]
    local grid_key = keys[2]
    local zone     = args[1]
    local eid      = args[2]
    local etype    = args[3]
    local sx, sy   = args[4], args[5]
    local gx, gy   = args[6], args[7]

    local spawn_ok = redis.call('FLECS.SPAWN', world, zone, eid, etype, sx, sy, '0', '0', '100')
    local path     = {}
    if gx and gy then
        path = redis.call('CYPATH.FIND', grid_key,
            tostring(math.floor(tonumber(sx))),
            tostring(math.floor(tonumber(sy))),
            tostring(math.floor(tonumber(gx))),
            tostring(math.floor(tonumber(gy))),
            '512')
    end
    return {spawn_ok, path}
end)

-- Plan + tick: run AI planning then advance simulation.
-- keys[1]=world_id keys[2]=ws_key keys[3]=actions_key
-- args[1]=zone args[2]=goal_fact args[3]=goal_val args[4]=dt_ms
redis.register_function('chain_plan_and_tick', function(keys, args)
    local world       = keys[1]
    local ws_key      = keys[2]
    local actions_key = keys[3]
    local zone        = args[1]
    local goal_fact   = args[2]
    local goal_val    = args[3]
    local dt          = args[4] or '50'

    local plan   = redis.call('CYGOAP.PLAN', ws_key, actions_key, goal_fact, goal_val, '10')
    local tick_r = redis.call('FLECS.TICK', world, zone, dt, '256')
    return {plan, tick_r}
end)
