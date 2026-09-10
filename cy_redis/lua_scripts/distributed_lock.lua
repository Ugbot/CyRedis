-- CyRedis Lua Script: Enhanced Distributed Lock
-- Implements Redlock algorithm with lease extension and deadlock prevention

local lock_key = KEYS[1]
local lock_value = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local extend_only = ARGV[3] == 'extend'
local force_unlock = ARGV[4] == 'force'

-- Check current lock status
local current_value = redis.call('GET', lock_key)

if force_unlock then
    -- Force unlock (admin operation)
    if current_value then
        redis.call('DEL', lock_key)
        return {unlocked=true, was_locked=true, old_value=current_value}
    else
        return {unlocked=false, was_locked=false}
    end
elseif extend_only then
    -- Extend existing lock
    if current_value == lock_value then
        redis.call('PEXPIRE', lock_key, ttl_ms)
        return {extended=true, ttl_ms=ttl_ms}
    else
        return {extended=false, current_owner=current_value}
    end
else
    -- Acquire new lock
    if current_value == nil then
        -- Lock is free
        redis.call('SET', lock_key, lock_value, 'PX', ttl_ms)
        return {acquired=true, ttl_ms=ttl_ms}
    elseif current_value == lock_value then
        -- We already own the lock, extend it
        redis.call('PEXPIRE', lock_key, ttl_ms)
        return {acquired=true, extended=true, ttl_ms=ttl_ms}
    else
        -- Lock owned by someone else
        local remaining_ttl = redis.call('PTTL', lock_key)
        return {acquired=false, owner=current_value, ttl_remaining_ms=remaining_ttl}
    end
end

