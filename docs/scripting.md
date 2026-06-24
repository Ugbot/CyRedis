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
import time

# Load at startup
with open("lua_scripts/rate_limiter.lua") as f:
    rate_sha = client.script_load(f.read())

# Use per-request: KEYS[1]=bucket key; ARGV = limit, window_seconds, now
allowed = client.evalsha(
    rate_sha, 1, "rl:user:u1", "10", "60", str(int(time.time()))
)
```

See [`lua_scripts/README.md`](../lua_scripts/README.md) for full argument documentation.

## Script manager

`CyLuaScriptManager` (in `cy_redis.features`) handles loading, caching, version
metadata, and atomic deployment of named scripts.

```python
from cy_redis.features import CyLuaScriptManager

mgr = CyLuaScriptManager(client, namespace="scripts")

# Register a script under a name (loads it, caches the SHA)
mgr.register_script("echo", "return ARGV[1]")

# Or load from a file
mgr.load_script_from_file("rate_limiter", "lua_scripts/rate_limiter.lua")

# Execute by name (args match the script; rate_limiter wants limit/window/now)
import time
result = mgr.execute_script(
    "rate_limiter", keys=["rl:user:u1"], args=["10", "60", str(int(time.time()))]
)

# Inspect and manage
mgr.list_scripts()              # {name: {sha, version, cached, ...}}
mgr.validate_script("return 1") # syntax-check without permanent load
mgr.reload_all_scripts()        # re-load every registered script

# All-or-nothing deployment of a set of scripts
mgr.atomic_deploy_scripts({"rate_limiter": {"source": "...", "version": "1.1.0"}})
```

## Redis Functions (FUNCTION LOAD / FCALL)

Redis 7.0+ supports named function libraries that persist across server
restarts. The client does **not** expose `function_load`/`fcall` directly.
Instead, `CyRedisFunctionsManager` (in `cy_redis.features`) ships a set of
curated built-in libraries — `cy:locks`, `cy:sema`, `cy:rate`, `cy:queue` —
and loads/dispatches them for you. (CyRedis also uses Functions internally for
the channel routing logic.)

```python
from cy_redis.features import CyRedisFunctionsManager

fm = CyRedisFunctionsManager(client)

# Load the built-in libraries (FUNCTION LOAD REPLACE under the hood)
fm.load_all_libraries()                 # or fm.load_library("cy:rate")
fm.list_loaded_libraries()              # ['cy:locks', 'cy:sema', 'cy:rate', 'cy:queue']
fm.get_library_info("cy:rate")

# Call a registered function by its full name: call_function(name, keys, args)
result = fm.call_function("cy_rate_token_bucket", ["rl:u1"], ["10", "1", "1"])
# token-bucket reply: [allowed, remaining, reset_ms, retry_after]
```

A higher-level facade `RedisFunctions(client)` is also exported from the top-level
package; it wraps the manager and adds `.locks`, `.rate`, and `.queue` helpers.

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
