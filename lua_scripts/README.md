# CyRedis Lua Scripts Library

This directory contains pre-built Lua scripts that provide advanced functionality for Redis operations. These scripts are designed to be loaded and executed by the CyRedis `LuaScriptManager`.

## Available Scripts

### 1. `rate_limiter.lua` - Advanced Rate Limiting
Implements a sophisticated rate limiting algorithm combining sliding window and burst capabilities.

**Features:**
- Sliding window rate limiting with configurable burst allowance
- Automatic cleanup of old entries
- Returns remaining capacity for client-side decision making

**Usage:**
```lua
local result = redis.call('EVALSHA', sha, 1, 'user:123:requests', 100, 3600, current_time, 200)
-- Returns remaining capacity (0 = rate limited)
```

### 2. `distributed_lock.lua` - Enhanced Distributed Locks
Implements the Redlock algorithm with additional features like lease extension and force unlock.

**Features:**
- Standard lock acquisition with TTL
- Lock extension for long-running operations
- Force unlock for administrative purposes
- Detailed status reporting

**Usage:**
```lua
-- Acquire lock
local result = redis.call('EVALSHA', sha, 1, 'lock:key', 'unique_value', 30000)

-- Extend lock
local result = redis.call('EVALSHA', sha, 1, 'lock:key', 'unique_value', 30000, 'extend')

-- Force unlock
local result = redis.call('EVALSHA', sha, 1, 'lock:key', '', 0, '', 'force')
```

### 3. `smart_cache.lua` - Intelligent Multi-Level Caching
Provides sophisticated caching with statistics, eviction policies, and TTL management.

**Features:**
- LRU eviction policy
- Cache hit/miss statistics
- Configurable cache size limits
- Comprehensive statistics reporting

**Usage:**
```lua
-- Get from cache
local result = redis.call('EVALSHA', sha, 3, 'cache:key', 'access:log', 'stats:key', 'GET', current_time)

-- Set cache with TTL
local result = redis.call('EVALSHA', sha, 3, 'cache:key', 'access:log', 'stats:key', 'SET', current_time, 'value', 3600, 1000)

-- Get statistics
local stats = redis.call('EVALSHA', sha, 3, 'cache:key', 'access:log', 'stats:key', 'STATS', current_time)
```

### 4. `job_queue.lua` - Advanced Job Queue Management
Implements a complete job queue system with priorities, retries, and dead letter queues.

**Features:**
- Priority-based job scheduling
- Delayed job execution
- Automatic retry with exponential backoff
- Dead letter queue for failed jobs
- Comprehensive queue statistics

**Usage:**
```lua
-- Push job with priority
local result = redis.call('EVALSHA', sha, 4, 'queue', 'processing', 'failed', 'dead', 'PUSH', current_time, 'job_123', 'job_data', 5, 3, 0)

-- Pop jobs
local jobs = redis.call('EVALSHA', sha, 4, 'queue', 'processing', 'failed', 'dead', 'POP', current_time, 5)

-- Mark job complete
local result = redis.call('EVALSHA', sha, 4, 'queue', 'processing', 'failed', 'dead', 'COMPLETE', current_time, 'job_123')

-- Mark job failed (with retry)
local result = redis.call('EVALSHA', sha, 4, 'queue', 'processing', 'failed', 'dead', 'FAIL', current_time, 'job_123', 'error_message')

-- Get queue stats
local stats = redis.call('EVALSHA', sha, 4, 'queue', 'processing', 'failed', 'dead', 'STATS', current_time)
```

## Loading Scripts in CyRedis

```python
from cy_redis import HighPerformanceRedis, LuaScriptManager

# Load a script from file
with HighPerformanceRedis() as redis:
    script_manager = LuaScriptManager(redis)

    # Load script from file
    with open('lua_scripts/rate_limiter.lua', 'r') as f:
        script_content = f.read()

    script_manager.register_script('rate_limiter', script_content)

    # Execute the loaded script
    result = script_manager.execute_script('rate_limiter', keys=['user:123'], args=[100, 3600, int(time.time()), 200])
    print(f"Rate limiter result: {result}")
```

## Script Development Guidelines

1. **Atomicity**: All operations within a script should be atomic
2. **Error Handling**: Scripts should return meaningful error information
3. **Performance**: Minimize the number of Redis calls within scripts
4. **Documentation**: Include comprehensive comments and usage examples
5. **Testing**: Each script should be thoroughly tested for edge cases

## Best Practices

- Use descriptive key names that include namespaces
- Implement proper cleanup mechanisms for expired data
- Return structured data (tables) for complex results
- Include operation timeouts and circuit breakers where appropriate
- Document all script parameters and return values

## Contributing

When adding new scripts:

1. Follow the naming convention: `snake_case.lua`
2. Include comprehensive documentation in the script comments
3. Add the script to this README with usage examples
4. Create corresponding Python wrapper methods in `ScriptHelper` class
5. Add tests and examples in the main test suite

