# Lua scripts

← [README](../README.md) | [Scripting guide](../docs/scripting.md)

Pre-built scripts for common patterns. Load with `client.script_load()` or the `ScriptManager`. See [docs/scripting.md](../docs/scripting.md) for the full scripting guide.

## rate_limiter.lua

Sliding-window rate limit. Returns remaining capacity (0 = denied).

```
Keys[1]: rate limit key (e.g. "rl:user:123")
Args: max_requests, window_seconds, current_time, burst_allowance
Returns: remaining capacity as integer
```

```python
sha = client.script_load(open("lua_scripts/rate_limiter.lua").read())
remaining = client.evalsha(sha, 1, "rl:user:123", "100", "3600", str(int(time.time())), "200")
```

## distributed_lock.lua

Redlock-style distributed lock with lease extension and force-unlock.

```
Keys[1]: lock key
Args: token, ttl_ms [, "extend" | "force"]
Returns: 1 (acquired/extended), 0 (failed)
```

```python
sha = client.script_load(open("lua_scripts/distributed_lock.lua").read())
# Acquire
ok = client.evalsha(sha, 1, "lock:billing", my_token, "30000")
# Extend
ok = client.evalsha(sha, 1, "lock:billing", my_token, "30000", "extend")
# Release
client.evalsha(sha, 1, "lock:billing", my_token, "0")
```

For application-level distributed locks see [docs/advanced.md](../docs/advanced.md).

## smart_cache.lua

Get-or-set with LRU eviction, hit/miss stats, and stampede protection.

```
Keys[1]: cache key, Keys[2]: access log, Keys[3]: stats key
Args: "GET"|"SET"|"STATS", current_time [, value, ttl, max_size]
```

```python
sha = client.script_load(open("lua_scripts/smart_cache.lua").read())
val = client.evalsha(sha, 3, "cache:obj", "cache:access", "cache:stats",
                     "GET", str(int(time.time())))
```

## job_queue.lua

Priority queue with delayed execution, retries (exponential backoff), and dead-letter queue.

```
Keys[1]: queue, Keys[2]: processing, Keys[3]: failed, Keys[4]: dead_letter
Args: "PUSH"|"POP"|"COMPLETE"|"FAIL"|"STATS", current_time, [job_id, data, priority, retries, delay]
```

```python
sha = client.script_load(open("lua_scripts/job_queue.lua").read())
# Enqueue
client.evalsha(sha, 4, "q:jobs", "q:proc", "q:failed", "q:dead",
               "PUSH", str(now), "job_1", json.dumps(payload), "5", "3", "0")
# Dequeue (up to 5 jobs)
jobs = client.evalsha(sha, 4, "q:jobs", "q:proc", "q:failed", "q:dead",
                      "POP", str(now), "5")
# Acknowledge
client.evalsha(sha, 4, "q:jobs", "q:proc", "q:failed", "q:dead",
               "COMPLETE", str(now), "job_1")
```
