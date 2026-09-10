-- CyRedis Lua Script: Advanced Rate Limiter
-- This script implements a sophisticated rate limiting algorithm
-- combining sliding window and token bucket approaches

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_size = tonumber(ARGV[2])
local current_time = tonumber(ARGV[3])
local burst_limit = tonumber(ARGV[4] or limit * 2)  -- Allow bursting up to 2x limit

-- Clean up old entries outside the window
redis.call('ZREMRANGEBYSCORE', key, 0, current_time - window_size)

-- Get current request count in window
local current_count = redis.call('ZCARD', key)

-- Check if we're within the sustained rate limit
if current_count >= limit then
    -- Check if we can allow bursting
    if current_count >= burst_limit then
        return 0  -- Rate limited
    end
end

-- Allow the request and record it
redis.call('ZADD', key, current_time, current_time .. ':' .. tostring(math.random()))
redis.call('EXPIRE', key, window_size * 2)  -- Extra time for cleanup

-- Return remaining capacity
local new_count = redis.call('ZCARD', key)
return math.max(0, burst_limit - new_count)

