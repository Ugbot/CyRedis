# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
CyRedis Standard Library of Redis Functions
High-performance, atomic operations using Redis 7+ Functions
"""

import json
import time
import hashlib
from typing import Dict, List, Any, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor

# Import our optimized Redis client
from cy_redis.core.cy_redis_client cimport CyRedisClient

# Function library manifest — keys map to Redis library names (underscores, no colons)
FUNCTION_LIBRARIES = {
    "cy:locks": {
        "version": "1.0.0",
        "lua_name": "cy_locks",
        "description": "Distributed reentrant locks with fencing tokens",
        "functions": ["cy_locks_acquire", "cy_locks_release", "cy_locks_refresh"]
    },
    "cy:sema": {
        "version": "1.0.0",
        "lua_name": "cy_sema",
        "description": "Semaphores and countdown latches",
        "functions": ["cy_sema_acquire", "cy_sema_release", "cy_latch_await", "cy_latch_countdown"]
    },
    "cy:rate": {
        "version": "1.0.0",
        "lua_name": "cy_rate",
        "description": "Rate limiting: token bucket, sliding window, leaky bucket",
        "functions": ["cy_rate_token_bucket", "cy_rate_sliding_window", "cy_rate_leaky_bucket"]
    },
    "cy:queue": {
        "version": "1.0.0",
        "lua_name": "cy_queue",
        "description": "Reliable queues with deduplication, delay, priorities, and DLQ",
        "functions": ["cy_queue_enqueue", "cy_queue_pull", "cy_queue_ack", "cy_queue_nack"]
    },
}

# Helper Lua snippet: compute millisecond timestamp from Redis TIME command
_LUA_NOW_MS = """
local _t = redis.call('TIME')
local now_ms = tonumber(_t[1]) * 1000 + math.floor(tonumber(_t[2]) / 1000)
"""

# Redis Functions source code — each block is a complete loadable library.
# Library names use underscores; function names are globally unique within Redis.
LOCKS_FUNCTIONS = """#!lua name=cy_locks

-- cy_locks: Distributed reentrant locks with fencing tokens and fair queuing
local function now_ms()
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

redis.register_function('cy_locks_acquire', function(keys, args)
    local k, owner, ttl = keys[1], args[1], tonumber(args[2])
    local fair_qkey = args[3] ~= '' and args[3] or nil
    local now = now_ms()

    local h = redis.call('HGETALL', k)
    if #h == 0 then
        local token = redis.call('INCR', 'cy:ftoken:locks')
        redis.call('HSET', k, 'owner', owner, 'count', 1,
                   'deadline', now + ttl, 'fencing_token', token)
        redis.call('PEXPIRE', k, ttl)
        return {1, token}
    end

    local cur_owner = h[2]
    if cur_owner == owner then
        local cnt = tonumber(h[4]) + 1
        local token = tonumber(h[10] or 0)
        redis.call('HSET', k, 'count', cnt, 'deadline', now + ttl)
        redis.call('PEXPIRE', k, ttl)
        return {1, token}
    end

    if fair_qkey then redis.call('LPUSH', fair_qkey, owner) end
    return {0, 0}
end)

redis.register_function('cy_locks_release', function(keys, args)
    local k, owner = keys[1], args[1]
    local fair_qkey = args[2] ~= '' and args[2] or nil

    local h = redis.call('HGETALL', k)
    if #h == 0 then return 0 end
    if h[2] ~= owner then return 0 end

    local cnt = tonumber(h[4]) - 1
    if cnt <= 0 then
        redis.call('DEL', k)
        if fair_qkey then
            local next_owner = redis.call('RPOP', fair_qkey)
            if next_owner then return 2 end
        end
        return 1
    end

    redis.call('HSET', k, 'count', cnt)
    return 1
end)

redis.register_function('cy_locks_refresh', function(keys, args)
    local k, owner, ttl = keys[1], args[1], tonumber(args[2])
    local now = now_ms()

    local cur_owner = redis.call('HGET', k, 'owner')
    if not cur_owner or cur_owner ~= owner then return 0 end

    redis.call('HSET', k, 'deadline', now + ttl)
    redis.call('PEXPIRE', k, ttl)
    return 1
end)
"""

SEMA_FUNCTIONS = """#!lua name=cy_sema

-- cy_sema: Semaphores and countdown latches
redis.register_function('cy_sema_acquire', function(keys, args)
    local k, owner, n, ttl, limit =
        keys[1], args[1], tonumber(args[2]), tonumber(args[3]), tonumber(args[4])
    local permits_key = k .. ':permits'

    local current = 0
    local permits = redis.call('HGETALL', permits_key)
    for i = 1, #permits, 2 do
        current = current + tonumber(permits[i+1])
    end

    if current + n > limit then
        return {0, limit - current}
    end

    local owner_permits = tonumber(redis.call('HGET', permits_key, owner) or 0)
    redis.call('HSET', permits_key, owner, owner_permits + n)
    redis.call('PEXPIRE', permits_key, ttl)
    return {1, limit - (current + n)}
end)

redis.register_function('cy_sema_release', function(keys, args)
    local k, owner, n = keys[1], args[1], tonumber(args[2])
    local permits_key = k .. ':permits'

    local owner_permits = tonumber(redis.call('HGET', permits_key, owner) or 0)
    if owner_permits < n then return 0 end

    owner_permits = owner_permits - n
    if owner_permits <= 0 then
        redis.call('HDEL', permits_key, owner)
    else
        redis.call('HSET', permits_key, owner, owner_permits)
    end
    return 1
end)

redis.register_function('cy_latch_await', function(keys, args)
    local k, target = keys[1], tonumber(args[1])
    local current = tonumber(redis.call('GET', k) or 0)
    return current <= target and 1 or 0
end)

redis.register_function('cy_latch_countdown', function(keys, args)
    local k = keys[1]
    local remaining = redis.call('DECR', k)
    if remaining <= 0 then
        redis.call('DEL', k)
        redis.call('PUBLISH', k .. ':ready', '1')
        return 1
    end
    return 0
end)
"""

RATE_FUNCTIONS = """#!lua name=cy_rate

-- cy_rate: Token bucket, sliding window, and leaky bucket rate limiters
local function now_ms()
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

redis.register_function('cy_rate_token_bucket', function(keys, args)
    local k, cap, refill_ms, cost =
        keys[1], tonumber(args[1]), tonumber(args[2]), tonumber(args[3])
    local now = now_ms()

    local last   = tonumber(redis.call('HGET', k, 'last')   or now)
    local tokens = tonumber(redis.call('HGET', k, 'tokens') or cap)

    local elapsed = now - last
    local refilled = math.floor(elapsed / refill_ms)
    tokens = math.min(cap, tokens + refilled)

    local allowed, retry_after = 0, 0
    if tokens >= cost then
        tokens = tokens - cost
        allowed = 1
    else
        retry_after = math.ceil((cost - tokens) * refill_ms)
    end

    redis.call('HSET', k, 'tokens', tokens, 'last', now)
    redis.call('PEXPIRE', k, math.max(refill_ms * cap * 2, 60000))

    local reset_ms = now + (cap - tokens) * refill_ms
    return {allowed, tokens, reset_ms, retry_after}
end)

redis.register_function('cy_rate_sliding_window', function(keys, args)
    local k, window_ms, max_req = keys[1], tonumber(args[1]), tonumber(args[2])
    local now = now_ms()

    redis.call('ZREMRANGEBYSCORE', k, 0, now - window_ms)
    local current = redis.call('ZCARD', k)

    local allowed, retry_after = 0, 0
    if current < max_req then
        local member = now .. ':' .. redis.call('INCR', k .. ':seq')
        redis.call('ZADD', k, now, member)
        redis.call('PEXPIRE', k, window_ms * 2)
        allowed = 1
    else
        local oldest = redis.call('ZRANGE', k, 0, 0, 'WITHSCORES')
        if #oldest >= 2 then
            retry_after = (tonumber(oldest[2]) + window_ms) - now
        end
    end

    local remaining = math.max(0, max_req - current - (allowed == 1 and 1 or 0))
    return {allowed, remaining, retry_after}
end)

redis.register_function('cy_rate_leaky_bucket', function(keys, args)
    local k, rate_per_ms, burst, cost =
        keys[1], tonumber(args[1]), tonumber(args[2]), tonumber(args[3])
    local now = now_ms()

    local last  = tonumber(redis.call('HGET', k, 'last')  or now)
    local level = tonumber(redis.call('HGET', k, 'level') or 0)

    local elapsed = now - last
    level = math.max(0, level - elapsed * rate_per_ms)

    local allowed, retry_after = 0, 0
    if level + cost <= burst then
        level = level + cost
        allowed = 1
    else
        retry_after = math.ceil((level + cost - burst) / rate_per_ms)
    end

    redis.call('HSET', k, 'level', level, 'last', now)
    redis.call('PEXPIRE', k, math.max(60000, math.ceil(burst / rate_per_ms) * 2))
    return {allowed, burst - level, retry_after}
end)
"""

QUEUE_FUNCTIONS = """#!lua name=cy_queue

-- cy_queue: Reliable queues with deduplication, delay, priorities, and DLQ
local function now_sec()
    return tonumber(redis.call('TIME')[1])
end

redis.register_function('cy_queue_enqueue', function(keys, args)
    local name, msg_id, payload, delay_s, ttl_ms, priority =
        args[1], args[2], args[3],
        tonumber(args[4] or 0), tonumber(args[5] or 0), tonumber(args[6] or 0)

    local ready_key   = 'cy:q:' .. name .. ':ready'
    local dedup_key   = 'cy:q:' .. name .. ':seen'
    local delayed_key = 'cy:q:' .. name .. ':delayed'

    if msg_id and msg_id ~= '' then
        if redis.call('SISMEMBER', dedup_key, msg_id) == 1 then return 0 end
        redis.call('SADD', dedup_key, msg_id)
        if ttl_ms > 0 then redis.call('PEXPIRE', dedup_key, ttl_ms) end
    end

    local now = now_sec()
    local msg = cjson.encode({id=msg_id, payload=payload,
                               enqueued_at=now, priority=priority})

    if delay_s > 0 then
        redis.call('ZADD', delayed_key, now + delay_s, msg)
        redis.call('EXPIREAT', delayed_key, now + delay_s + 3600)
        return 2
    end

    if priority > 0 then
        redis.call('LPUSH', ready_key .. ':p' .. priority, msg)
    else
        redis.call('LPUSH', ready_key, msg)
    end
    return 1
end)

redis.register_function('cy_queue_pull', function(keys, args)
    local name, vis_s, max_n = args[1], tonumber(args[2]), tonumber(args[3])
    local now = now_sec()

    local ready_key   = 'cy:q:' .. name .. ':ready'
    local inflight_key= 'cy:q:' .. name .. ':inflight'
    local delayed_key = 'cy:q:' .. name .. ':delayed'

    local due = redis.call('ZRANGEBYSCORE', delayed_key, 0, now, 'LIMIT', 0, max_n)
    for _, msg in ipairs(due) do
        redis.call('LPUSH', ready_key, msg)
        redis.call('ZREM', delayed_key, msg)
    end

    local result = {}
    for i = 1, max_n do
        local msg = redis.call('RPOP', ready_key)
        if not msg then break end
        redis.call('ZADD', inflight_key, now + vis_s, msg)
        table.insert(result, msg)
    end
    return result
end)

redis.register_function('cy_queue_ack', function(keys, args)
    local name, msg_id = args[1], args[2]
    local inflight_key = 'cy:q:' .. name .. ':inflight'

    local messages = redis.call('ZRANGE', inflight_key, 0, -1)
    for _, msg_json in ipairs(messages) do
        local ok, msg = pcall(cjson.decode, msg_json)
        if ok and msg.id == msg_id then
            redis.call('ZREM', inflight_key, msg_json)
            return 1
        end
    end
    return 0
end)

redis.register_function('cy_queue_nack', function(keys, args)
    local name, msg_id, requeue = args[1], args[2], args[3] == '1'
    local inflight_key = 'cy:q:' .. name .. ':inflight'
    local ready_key    = 'cy:q:' .. name .. ':ready'
    local dlq_key      = 'cy:q:' .. name .. ':dlq'

    local messages = redis.call('ZRANGE', inflight_key, 0, -1)
    for _, msg_json in ipairs(messages) do
        local ok, msg = pcall(cjson.decode, msg_json)
        if ok and msg.id == msg_id then
            redis.call('ZREM', inflight_key, msg_json)
            if requeue then
                redis.call('LPUSH', ready_key, msg_json)
                return 1
            else
                redis.call('LPUSH', dlq_key, msg_json)
                return 2
            end
        end
    end
    return 0
end)
"""

_LIBRARY_CODE = {
    "cy:locks": LOCKS_FUNCTIONS,
    "cy:sema":  SEMA_FUNCTIONS,
    "cy:rate":  RATE_FUNCTIONS,
    "cy:queue": QUEUE_FUNCTIONS,
}

# Function manager
cdef class CyRedisFunctionsManager:
    """
    High-performance Redis Functions manager with auto-loading and versioning
    """

    cdef public object redis
    cdef dict loaded_libraries
    cdef object executor

    def __cinit__(self, redis_client):
        self.redis = redis_client
        self.loaded_libraries = {}
        self.executor = ThreadPoolExecutor(max_workers=2)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef dict load_library(self, str library_name, str version="1.0.0"):
        """
        Load a Redis Functions library
        """
        if library_name not in FUNCTION_LIBRARIES:
            raise ValueError(f"Unknown library: {library_name}")

        lib_info = FUNCTION_LIBRARIES[library_name]

        # Check if already loaded with correct version
        try:
            existing = self.redis.execute_command(['FUNCTION', 'LIST', 'LIBRARYNAME', library_name])
            if existing and len(existing) > 0:
                # Check version (simplified)
                self.loaded_libraries[library_name] = lib_info
                return {'status': 'already_loaded', 'version': version}
        except Exception:
            pass  # Not loaded yet

        # Load the library
        code = self._get_library_code(library_name)
        if not code:
            raise ValueError(f"No code available for library: {library_name}")

        try:
            result = self.redis.execute_command(['FUNCTION', 'LOAD', 'REPLACE', code])
            self.loaded_libraries[library_name] = lib_info
            return {'status': 'loaded', 'version': version, 'functions': lib_info['functions']}
        except Exception as e:
            raise Exception(f"Failed to load library {library_name}: {e}")

    cdef str _get_library_code(self, str library_name):
        """Get the Lua code for a library"""
        return _LIBRARY_CODE.get(library_name, "")

    cpdef dict load_all_libraries(self):
        """Load all available function libraries"""
        results = {}
        for lib_name in FUNCTION_LIBRARIES.keys():
            try:
                result = self.load_library(lib_name)
                results[lib_name] = result
            except Exception as e:
                results[lib_name] = {'status': 'failed', 'error': str(e)}

        return results

    cpdef object call_function(self, str full_function_name, list keys=None, list args=None):
        """Call a Redis Function by its full registered name.

        Format: FCALL function_name numkeys [key ...] [arg ...]
        """
        cdef list keys_list = keys if keys is not None else []
        cdef list args_list = args if args is not None else []
        return self.redis.execute_command(
            ['FCALL', full_function_name, str(len(keys_list))] + keys_list + args_list
        )

    cpdef dict get_library_info(self, str library_name):
        """Get information about a loaded library"""
        if library_name not in self.loaded_libraries:
            return {}

        lib_info = self.loaded_libraries[library_name]

        # Get function list from Redis
        try:
            func_list = self.redis.execute_command(['FUNCTION', 'LIST', 'LIBRARYNAME', library_name])
            return {
                'name': library_name,
                'version': lib_info['version'],
                'description': lib_info['description'],
                'functions': lib_info['functions'],
                'redis_functions': func_list
            }
        except Exception:
            return {
                'name': library_name,
                'version': lib_info['version'],
                'description': lib_info['description'],
                'functions': lib_info['functions']
            }

    cpdef list list_loaded_libraries(self):
        """List all loaded function libraries"""
        return list(self.loaded_libraries.keys())

# High-level convenience classes for each domain

cdef class CyLocks:
    """Distributed locks with reentrancy and fencing tokens"""

    cdef CyRedisFunctionsManager func_mgr

    def __init__(self, func_mgr):
        self.func_mgr = func_mgr

    def acquire(self, key: str, owner: str, ttl_ms: int = 30000, fair_queue: str = None):
        """Acquire a distributed reentrant lock. Returns dict with acquired + fencing_token."""
        lock_key = f"cy:lock:{{{key}}}"
        fair_key = f"cy:lockq:{{{key}}}" if fair_queue else ""
        result = self.func_mgr.call_function(
            "cy_locks_acquire",
            keys=[lock_key],
            args=[owner, str(ttl_ms), fair_key],
        )
        if result and len(result) >= 2:
            return {'acquired': result[0] == 1, 'fencing_token': result[1]}
        return {'acquired': False, 'fencing_token': 0}

    def release(self, key: str, owner: str, fair_queue: str = None):
        """Release a distributed lock."""
        lock_key = f"cy:lock:{{{key}}}"
        fair_key = f"cy:lockq:{{{key}}}" if fair_queue else ""
        result = self.func_mgr.call_function(
            "cy_locks_release",
            keys=[lock_key],
            args=[owner, fair_key],
        )
        return result in (1, 2)

    def refresh(self, key: str, owner: str, ttl_ms: int = 30000):
        """Extend a held lock's TTL."""
        result = self.func_mgr.call_function(
            "cy_locks_refresh",
            keys=[f"cy:lock:{{{key}}}"],
            args=[owner, str(ttl_ms)],
        )
        return result == 1

cdef class CyRateLimiter:
    """Rate limiting with multiple algorithms"""

    cdef CyRedisFunctionsManager func_mgr

    def __init__(self, func_mgr):
        self.func_mgr = func_mgr

    def token_bucket(self, key: str, capacity: int, refill_interval_ms: int, cost: int = 1):
        """Token bucket rate limiting. refill_interval_ms = ms between each +1 token refill."""
        result = self.func_mgr.call_function(
            "cy_rate_token_bucket",
            keys=[f"cy:rate:{{{key}}}"],
            args=[str(capacity), str(refill_interval_ms), str(cost)],
        )
        if result and len(result) >= 4:
            return {
                'allowed': result[0] == 1,
                'remaining': result[1],
                'reset_ms': result[2],
                'retry_after_ms': result[3],
            }
        return {'allowed': False, 'remaining': 0, 'retry_after_ms': 1000}

    def sliding_window(self, key: str, window_ms: int, max_requests: int):
        """Sliding window rate limiting."""
        result = self.func_mgr.call_function(
            "cy_rate_sliding_window",
            keys=[f"cy:rate:{{{key}}}"],
            args=[str(window_ms), str(max_requests)],
        )
        if result and len(result) >= 3:
            return {
                'allowed': result[0] == 1,
                'remaining': result[1],
                'retry_after_ms': result[2],
            }
        return {'allowed': False, 'remaining': 0, 'retry_after_ms': 1000}

    def leaky_bucket(self, key: str, rate_per_ms: float, burst: int, cost: int = 1):
        """Leaky bucket rate limiting."""
        result = self.func_mgr.call_function(
            "cy_rate_leaky_bucket",
            keys=[f"cy:rate:{{{key}}}"],
            args=[str(rate_per_ms), str(burst), str(cost)],
        )
        if result and len(result) >= 3:
            return {
                'allowed': result[0] == 1,
                'remaining': result[1],
                'retry_after_ms': result[2],
            }
        return {'allowed': False, 'remaining': 0, 'retry_after_ms': 1000}

cdef class CyQueue:
    """Reliable key-based queues with deduplication"""

    cdef CyRedisFunctionsManager func_mgr

    def __init__(self, func_mgr):
        self.func_mgr = func_mgr

    def enqueue(self, name: str, message_id: str, payload: str,
                delay_s: int = 0, ttl_ms: int = 0, priority: int = 0):
        """Enqueue a message. Returns 'enqueued', 'delayed', or 'duplicate'."""
        result = self.func_mgr.call_function(
            "cy_queue_enqueue",
            args=[name, message_id, payload, str(delay_s), str(ttl_ms), str(priority)],
        )
        if result == 1:
            return "enqueued"
        elif result == 2:
            return "delayed"
        return "duplicate"

    def pull(self, name: str, visibility_s: int = 30, max_messages: int = 1):
        """Pull messages from queue with visibility timeout (seconds)."""
        result = self.func_mgr.call_function(
            "cy_queue_pull",
            args=[name, str(visibility_s), str(max_messages)],
        )
        return result or []

    def ack(self, name: str, message_id: str):
        """Acknowledge successful message processing."""
        result = self.func_mgr.call_function("cy_queue_ack", args=[name, message_id])
        return result == 1

    def nack(self, name: str, message_id: str, requeue: bool = True):
        """Negative acknowledge — requeue or send to DLQ."""
        result = self.func_mgr.call_function(
            "cy_queue_nack",
            args=[name, message_id, "1" if requeue else "0"],
        )
        return "requeued" if result == 1 else "dlq" if result == 2 else "not_found"

# Python wrapper
class RedisFunctions:
    """
    CyRedis Standard Library of Redis Functions
    High-level Python API for atomic Redis operations
    """

    def __init__(self, redis_client):
        """
        redis_client must be a CyRedisClient instance (or an object whose
        ._client attribute is a CyRedisClient, for backwards compatibility).
        """
        from cy_redis.core.cy_redis_client import CyRedisClient as _CyRC
        raw = getattr(redis_client, '_client', redis_client)
        if not isinstance(raw, _CyRC):
            raise TypeError(
                f"redis_client must be a CyRedisClient, got {type(raw).__name__}"
            )
        self._func_mgr = CyRedisFunctionsManager(raw)
        self.locks = CyLocks(self._func_mgr)
        self.rate = CyRateLimiter(self._func_mgr)
        self.queue = CyQueue(self._func_mgr)

    def load_library(self, library_name: str):
        """Load a specific function library"""
        return self._func_mgr.load_library(library_name)

    def load_all_libraries(self):
        """Load all available function libraries"""
        return self._func_mgr.load_all_libraries()

    def call_function(self, library: str, function: str, keys=None, args=None):
        """Call a Redis Function directly"""
        return self._func_mgr.call_function(library, function, keys, args)

    def get_library_info(self, library_name: str):
        """Get information about a loaded library"""
        return self._func_mgr.get_library_info(library_name)

    def list_loaded_libraries(self):
        """List all loaded function libraries"""
        return self._func_mgr.list_loaded_libraries()
