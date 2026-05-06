# Scripting

← [README](../README.md) | [Core API](core-api.md) | [Advanced features](advanced.md)

CyRedis supports both classic Lua scripts (EVAL/EVALSHA) and the newer Redis Functions (FUNCTION LOAD / FCALL). Both execute atomically inside the Redis server — no round-trips between commands.

## Lua scripts (EVAL)

```python
# Run a script directly
result = client.eval(script, num_keys, *keys_and_args)

# Load a script and run by SHA (avoids re-sending script bytes each time)
sha = client.script_load(script)
result = client.evalsha(sha, num_keys, *keys_and_args)

# Flush all cached scripts
client.script_flush()
```

### Pre-built scripts

Four production-ready scripts live in [`lua_scripts/`](../lua_scripts/):

| Script | File | What it does |
|--------|------|-------------|
| Rate limiter | `rate_limiter.lua` | Sliding-window rate limit — returns 1 (allow) or 0 (deny) |
| Distributed lock | `distributed_lock.lua` | Try-acquire a lock with TTL; safe release checks token |
| Smart cache | `smart_cache.lua` | Get-or-set with stampede protection |
| Job queue | `job_queue.lua` | Atomic enqueue with deduplication |

```python
# Load at startup
with open("lua_scripts/rate_limiter.lua") as f:
    rate_sha = client.script_load(f.read())

# Use per-request
allowed = client.evalsha(rate_sha, 1, "rl:user:u1", "10", "60")
```

See [`lua_scripts/README.md`](../lua_scripts/README.md) for full argument documentation.

## Script manager

`ScriptManager` (in `cy_redis.features.script_manager`) handles loading, caching, and atomic deployment of multiple scripts. It also supports hot-reload without dropping connections.

```python
from cy_redis.features import ScriptManager

mgr = ScriptManager(client)
mgr.load_directory("lua_scripts/")   # loads all .lua files

# Call by name
result = mgr.call("rate_limiter", keys=["rl:user:u1"], args=["10", "60"])

# Atomic reload (all-or-nothing)
mgr.reload_all()
```

See [`examples/example_atomic_script_deployment.py`](../examples/example_atomic_script_deployment.py).

## Redis Functions (FUNCTION LOAD / FCALL)

Redis 7.0+ supports named function libraries that persist across server restarts. CyRedis uses Functions internally for the channel routing logic and exposes them for application use.

```python
# Load a function library
client.function_load(library_code)

# Call a function
result = client.fcall("my_function", num_keys, *keys_and_args)
result = client.fcall_ro("my_function", num_keys, *keys_and_args)  # read-only

# List loaded libraries
libraries = client.function_list()

# Delete a library
client.function_delete("my_lib_name")

# Dump and restore (for migration)
dump = client.function_dump()
client.function_restore(dump)
```

### Example library

```lua
#!lua name=mylib

redis.register_function('rate_check', function(keys, args)
    local current = redis.call('INCR', keys[1])
    if current == 1 then
        redis.call('EXPIRE', keys[1], tonumber(args[1]))
    end
    return current <= tonumber(args[2]) and 1 or 0
end)
```

```python
client.function_load(open("mylib.lua").read())
allowed = client.fcall("rate_check", 1, "rl:u1", "60", "10")
```

See [`examples/example_redis_functions.py`](../examples/example_redis_functions.py).

### Channel routing function

`CyChannelManager` loads the `cy_channel_route` function automatically at `start()`. It evaluates per-subscriber field-match filters entirely inside Redis — one `FCALL` call replaces N Python-side filter checks.

```lua
-- Stored in cy:chan:{channel}:routes as: {conn_id: json_filter_or_empty}
-- Called per published message; returns list of matching conn_ids
redis.register_function('cy_channel_route', function(keys, args)
    local routes = redis.call('HGETALL', keys[1])
    -- ... evaluate each route filter against the message data
    return matches
end)
```

See [`docs/web-channels.md`](web-channels.md) for the filter API.

## Transactions vs scripting

Use scripting when you need read-modify-write atomicity across multiple keys with conditional logic. Use MULTI/EXEC transactions when you have a fixed sequence of writes and optimistic locking via WATCH is enough.

| | MULTI/EXEC | EVAL/EVALSHA | FCALL |
|---|---|---|---|
| Atomic | Yes | Yes | Yes |
| Conditional logic | No | Yes | Yes |
| Persists across restart | No | No | Yes (Functions) |
| Works in cluster | No* | Depends on key slot | Depends |

*MULTI/EXEC in cluster mode requires all keys to be in the same slot.

## What to read next

- [Advanced features](advanced.md) — distributed locks built on Lua
- [Web channels](web-channels.md) — per-subscriber routing via FCALL
- [Core API](core-api.md) — EVAL signature details
