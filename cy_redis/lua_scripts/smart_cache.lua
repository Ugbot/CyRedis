-- CyRedis Lua Script: Smart Multi-Level Cache
-- Implements intelligent caching with TTL management and statistics

local cache_key = KEYS[1]
local access_log_key = KEYS[2]
local stats_key = KEYS[3]
local operation = ARGV[1]
local current_time = tonumber(ARGV[2])

if operation == 'GET' then
    -- Get from cache with statistics
    local data = redis.call('GET', cache_key)

    if data then
        -- Cache hit: update access time and increment hit counter
        redis.call('ZADD', access_log_key, current_time, cache_key)
        redis.call('HINCRBY', stats_key, 'hits', 1)
        return {hit=true, data=data}
    else
        -- Cache miss: increment miss counter
        redis.call('HINCRBY', stats_key, 'misses', 1)
        return {hit=false}
    end

elseif operation == 'SET' then
    -- Set cache with TTL and eviction policy
    local value = ARGV[3]
    local ttl_seconds = tonumber(ARGV[4])
    local max_entries = tonumber(ARGV[5] or 1000)

    -- Check cache size and evict if necessary (LRU)
    local current_size = redis.call('ZCARD', access_log_key)
    if current_size >= max_entries then
        local to_evict = redis.call('ZRANGE', access_log_key, 0, current_size - max_entries)
        for i, key in ipairs(to_evict) do
            redis.call('DEL', key)
        end
        redis.call('ZREMRANGEBYRANK', access_log_key, 0, current_size - max_entries)
    end

    -- Set the cache entry
    redis.call('SETEX', cache_key, ttl_seconds, value)
    redis.call('ZADD', access_log_key, current_time, cache_key)

    -- Update statistics
    redis.call('HINCRBY', stats_key, 'sets', 1)

    return {set=true, ttl_seconds=ttl_seconds}

elseif operation == 'DELETE' then
    -- Delete from cache
    local deleted = redis.call('DEL', cache_key)
    if deleted > 0 then
        redis.call('ZREM', access_log_key, cache_key)
        redis.call('HINCRBY', stats_key, 'deletes', 1)
    end
    return {deleted=deleted > 0}

elseif operation == 'STATS' then
    -- Get cache statistics
    local stats = redis.call('HGETALL', stats_key)
    local size = redis.call('ZCARD', access_log_key)
    stats.size = size

    -- Calculate hit rate
    local hits = tonumber(stats.hits or 0)
    local misses = tonumber(stats.misses or 0)
    local total = hits + misses
    if total > 0 then
        stats.hit_rate = hits / total
    else
        stats.hit_rate = 0
    end

    return stats

elseif operation == 'CLEAR' then
    -- Clear entire cache
    local keys = redis.call('ZRANGE', access_log_key, 0, -1)
    if #keys > 0 then
        redis.call('DEL', unpack(keys))
    end
    redis.call('DEL', access_log_key, stats_key)
    return {cleared=true, entries_cleared=#keys}

end

return {error="Unknown operation: " .. operation}

