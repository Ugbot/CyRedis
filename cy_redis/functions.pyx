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
from cy_redis.cy_redis_client import CyRedisClient

# Function library manifest
FUNCTION_LIBRARIES = {
    "cy:locks": {
        "version": "1.0.0",
        "description": "Distributed locks with reentrancy and fencing tokens",
        "functions": ["acquire", "release", "refresh", "try_multi"]
    },
    "cy:sema": {
        "version": "1.0.0",
        "description": "Semaphores and countdown latches",
        "functions": ["acquire", "release", "await", "countdown"]
    },
    "cy:rate": {
        "version": "1.0.0",
        "description": "Rate limiting algorithms (token bucket, sliding window, leaky bucket)",
        "functions": ["token_bucket", "sliding_window", "leaky_bucket"]
    },
    "cy:queue": {
        "version": "1.0.0",
        "description": "Reliable key-based queues with deduplication",
        "functions": ["enqueue", "pull", "ack", "nack", "extend"]
    },
    "cy:streamq": {
        "version": "1.0.0",
        "description": "Streams-based queues with exactly-once delivery",
        "functions": ["enqueue", "promote_scheduled", "claim_stale", "ack", "nack"]
    },
    "cy:cache": {
        "version": "1.0.0",
        "description": "Client-side caching helpers",
        "functions": ["get_or_set_json", "mget_or_mset"]
    },
    "cy:atomic": {
        "version": "1.0.0",
        "description": "Atomic key-value operations",
        "functions": ["cas", "incr_with_ttl", "msetnx_ttl"]
    },
    "cy:tokens": {
        "version": "1.0.0",
        "description": "ID generation and sequencing",
        "functions": ["ksortable", "next"]
    }
}

# Redis Functions source code
LOCKS_FUNCTIONS = """
-- cy:locks - Distributed locks with reentrancy and fencing tokens

redis.register_function('locks.acquire', function(keys, args)
    local k, owner, ttl = keys[1], args[1], tonumber(args[2])
    local fair_qkey = args[3]  -- optional fair queue key
    local now = redis.call('PTIME')

    local h = redis.call('HGETALL', k)
    if #h == 0 then
        -- new owner
        redis.call('HSET', k, 'owner', owner, 'count', 1, 'deadline', now + ttl * 1000)
        redis.call('PEXPIRE', k, ttl)
        local token = redis.call('INCR', 'cy:token:locks')
        return {1, token}  -- ok, fencing_token
    else
        local cur_owner = h[2]
        if cur_owner == owner then
            -- reentrant
            local cnt = tonumber(h[4]) + 1
            redis.call('HSET', k, 'count', cnt, 'deadline', now + ttl * 1000)
            redis.call('PEXPIRE', k, ttl)
            local token = redis.call('HGET', k, 'fencing_token') or '0'
            return {1, tonumber(token)}  -- ok, fencing_token
        else
            -- held by someone else
            if fair_qkey then
                redis.call('LPUSH', fair_qkey, owner)
            end
            return {0, 0}  -- not_ok, no_token
        end
    end
end)

redis.register_function('locks.release', function(keys, args)
    local k, owner = keys[1], args[1]
    local fair_qkey = args[2]  -- optional fair queue

    local h = redis.call('HGETALL', k)
    if #h == 0 then return 0 end  -- not held

    local cur_owner = h[2]
    if cur_owner ~= owner then return 0 end  -- wrong owner

    local cnt = tonumber(h[4]) - 1
    if cnt <= 0 then
        redis.call('DEL', k)
        -- promote next waiter
        if fair_qkey then
            local next_owner = redis.call('RPOP', fair_qkey)
            if next_owner then
                return 2  -- released_and_promoted
            end
        end
        return 1  -- released
    else
        redis.call('HSET', k, 'count', cnt)
        return 1  -- released
    end
end)

redis.register_function('locks.refresh', function(keys, args)
    local k, owner, ttl = keys[1], args[1], tonumber(args[2])
    local now = redis.call('PTIME')

    local h = redis.call('HGETALL', k)
    if #h == 0 then return 0 end

    local cur_owner = h[2]
    if cur_owner ~= owner then return 0 end

    redis.call('HSET', k, 'deadline', now + ttl * 1000)
    redis.call('PEXPIRE', k, ttl)
    return 1
end)
"""

SEMA_FUNCTIONS = """
-- cy:sema - Semaphores and countdown latches

redis.register_function('sema.acquire', function(keys, args)
    local k, owner, n, ttl, limit = keys[1], args[1], tonumber(args[2]), tonumber(args[3]), tonumber(args[4])
    local permits_key = k .. ':permits'
    local owner_key = permits_key .. ':' .. owner

    -- Check current total
    local current = 0
    local permits = redis.call('HGETALL', permits_key)
    for i = 1, #permits, 2 do
        current = current + tonumber(permits[i+1])
    end

    if current + n > limit then
        return {0, limit - current}  -- not_granted, remaining
    end

    -- Grant permits
    local owner_permits = tonumber(redis.call('HGET', permits_key, owner) or 0)
    redis.call('HSET', permits_key, owner, owner_permits + n)
    redis.call('PEXPIRE', owner_key, ttl)

    return {1, limit - (current + n)}  -- granted, remaining
end)

redis.register_function('sema.release', function(keys, args)
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

redis.register_function('latch.await', function(keys, args)
    local k, target = keys[1], tonumber(args[1])
    local current = tonumber(redis.call('GET', k) or 0)
    if current <= target then
        return 1  -- ready
    end
    return 0  -- not ready
end)

redis.register_function('latch.countdown', function(keys, args)
    local k = keys[1]
    local remaining = redis.call('DECR', k)
    if remaining <= 0 then
        redis.call('PUBLISH', k .. ':ready', '1')
        return 1  -- zero reached
    end
    return 0  -- still counting
end)
"""

RATE_FUNCTIONS = """
-- cy:rate - Rate limiting algorithms

redis.register_function('rate.token_bucket', function(keys, args)
    local k, now, cap, refill, cost = keys[1], tonumber(args[1]), tonumber(args[2]), tonumber(args[3]), tonumber(args[4])

    local last = tonumber(redis.call('HGET', k, 'last') or now)
    local tokens = tonumber(redis.call('HGET', k, 'tokens') or cap)

    -- Refill tokens
    local elapsed = now - last
    local refill_amount = math.floor(elapsed / refill)
    tokens = math.min(cap, tokens + refill_amount)

    local allowed = 0
    local retry_after = 0

    if tokens >= cost then
        tokens = tokens - cost
        allowed = 1
    else
        -- Calculate retry time
        local needed = cost - tokens
        retry_after = math.ceil(needed * refill)
    end

    -- Update state
    redis.call('HSET', k, 'tokens', tokens, 'last', now)
    redis.call('PEXPIRE', k, math.max(refill * cap * 2, 60000))  -- 2x capacity or 1min

    local reset_time = now + (cap - tokens) * refill
    return {allowed, tokens, reset_time, retry_after}
end)

redis.register_function('rate.sliding_window', function(keys, args)
    local k, now, window, max_req = keys[1], tonumber(args[1]), tonumber(args[2]), tonumber(args[3])

    -- Remove old entries
    redis.call('ZREMRANGEBYSCORE', k, 0, now - window)

    -- Count current requests
    local current = redis.call('ZCARD', k)

    local allowed = 0
    local retry_after = 0

    if current < max_req then
        redis.call('ZADD', k, now, now .. ':' .. tostring(math.random()))
        redis.call('PEXPIRE', k, window * 2)
        allowed = 1
    else
        -- Calculate retry time (simplified)
        local oldest = redis.call('ZRANGE', k, 0, 0, 'WITHSCORES')
        if #oldest > 0 then
            local oldest_time = tonumber(oldest[2])
            retry_after = (oldest_time + window) - now
        end
    end

    return {allowed, max_req - current - (allowed == 0 and 1 or 0), retry_after}
end)

redis.register_function('rate.leaky_bucket', function(keys, args)
    local k, now, rate, burst, cost = keys[1], tonumber(args[1]), tonumber(args[2]), tonumber(args[3]), tonumber(args[4])

    local last = tonumber(redis.call('HGET', k, 'last') or now)
    local level = tonumber(redis.call('HGET', k, 'level') or 0)

    -- Leak tokens
    local elapsed = now - last
    local leaked = elapsed * rate
    level = math.max(0, level - leaked)

    local allowed = 0
    local retry_after = 0

    if level + cost <= burst then
        level = level + cost
        allowed = 1
    else
        -- Calculate retry time
        local excess = level + cost - burst
        retry_after = math.ceil(excess / rate)
    end

    -- Update state
    redis.call('HSET', k, 'level', level, 'last', now)
    redis.call('PEXPIRE', k, math.max(60000, burst / rate * 2000))  -- Conservative TTL

    return {allowed, burst - level, retry_after}
end)
"""

QUEUE_FUNCTIONS = """
-- cy:queue - Reliable key-based queues

redis.register_function('queue.enqueue', function(keys, args)
    local name, msg_id, payload, delay, ttl, priority =
        args[1], args[2], args[3], tonumber(args[4] or 0), tonumber(args[5] or 0), tonumber(args[6] or 0)

    local ready_key = 'cy:q:' .. name .. ':ready'
    local dedup_key = 'cy:q:' .. name .. ':seen'
    local delayed_key = 'cy:q:' .. name .. ':delayed'

    -- Deduplication
    if msg_id and msg_id ~= '' then
        local seen = redis.call('SISMEMBER', dedup_key, msg_id)
        if seen == 1 then return 0 end  -- duplicate
        redis.call('SADD', dedup_key, msg_id)
        if ttl > 0 then redis.call('PEXPIRE', dedup_key, ttl) end
    end

    -- Create message envelope
    local msg = cjson.encode({
        id = msg_id,
        payload = payload,
        enqueued_at = redis.call('TIME')[1],
        priority = priority,
        ttl = ttl
    })

    if delay > 0 then
        local ready_time = redis.call('TIME')[1] + delay
        redis.call('ZADD', delayed_key, ready_time, msg)
        redis.call('PEXPIRE', delayed_key, delay + 3600000)  -- 1 hour grace
        return 2  -- delayed
    else
        if priority > 0 then
            redis.call('LPUSH', ready_key .. ':p' .. priority, msg)
        else
            redis.call('LPUSH', ready_key, msg)
        end
        return 1  -- ready
    end
end)

redis.register_function('queue.pull', function(keys, args)
    local name, now, vis_ms, max_n =
        args[1], tonumber(args[2]), tonumber(args[3]), tonumber(args[4])

    local ready_key = 'cy:q:' .. name .. ':ready'
    local inflight_key = 'cy:q:' .. name .. ':inflight'
    local delayed_key = 'cy:q:' .. name .. ':delayed'

    -- Promote delayed messages first
    local due = redis.call('ZRANGEBYSCORE', delayed_key, 0, now, 'LIMIT', 0, max_n)
    for _, msg in ipairs(due) do
        redis.call('LPUSH', ready_key, msg)
        redis.call('ZREM', delayed_key, msg)
    end

    -- Pull from ready queue
    local result = {}
    for i = 1, max_n do
        local msg = redis.call('RPOP', ready_key)
        if not msg then break end

        local deadline = now + vis_ms
        redis.call('ZADD', inflight_key, deadline, msg)
        table.insert(result, msg)
    end

    return result
end)

redis.register_function('queue.ack', function(keys, args)
    local name, msg_id = args[1], args[2]
    local inflight_key = 'cy:q:' .. name .. ':inflight'

    -- Find and remove the message (simplified - would need full scan in production)
    local messages = redis.call('ZRANGE', inflight_key, 0, -1)
    for _, msg_json in ipairs(messages) do
        local msg = cjson.decode(msg_json)
        if msg.id == msg_id then
            redis.call('ZREM', inflight_key, msg_json)
            return 1
        end
    end
    return 0
end)

redis.register_function('queue.nack', function(keys, args)
    local name, msg_id, requeue = args[1], args[2], args[3] == '1'
    local inflight_key = 'cy:q:' .. name .. ':inflight'
    local ready_key = 'cy:q:' .. name .. ':ready'
    local dlq_key = 'cy:q:' .. name .. ':dlq'

    -- Find and remove the message
    local messages = redis.call('ZRANGE', inflight_key, 0, -1)
    for _, msg_json in ipairs(messages) do
        local msg = cjson.decode(msg_json)
        if msg.id == msg_id then
            redis.call('ZREM', inflight_key, msg_json)
            if requeue then
                redis.call('LPUSH', ready_key, msg_json)
                return 1  -- requeued
            else
                redis.call('LPUSH', dlq_key, msg_json)
                return 2  -- dlq
            end
        end
    end
    return 0  -- not found
end)
"""

# Combine all function libraries
ALL_FUNCTIONS_CODE = LOCKS_FUNCTIONS + SEMA_FUNCTIONS + RATE_FUNCTIONS + QUEUE_FUNCTIONS

# Function manager
cdef class CyRedisFunctionsManager:
    """
    High-performance Redis Functions manager with auto-loading and versioning
    """

    cdef CyRedisClient redis
    cdef dict loaded_libraries
    cdef object executor

    def __cinit__(self, CyRedisClient redis_client):
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
        if library_name == "cy:locks":
            return LOCKS_FUNCTIONS
        elif library_name == "cy:sema":
            return SEMA_FUNCTIONS
        elif library_name == "cy:rate":
            return RATE_FUNCTIONS
        elif library_name == "cy:queue":
            return QUEUE_FUNCTIONS
        # Add other libraries as implemented
        return ""

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

    cpdef object call_function(self, str library_name, str function_name, list keys=None, list args=None):
        """Call a Redis Function"""
        full_name = f"{library_name}.{function_name}"
        return self.redis.execute_command(['FCALL', full_name] + (keys or []) + [len(args or [])] + (args or []))

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
        """Acquire a distributed lock"""
        keys = [f"cy:lock:{{{key}}}"]
        args = [owner, str(ttl_ms)]
        if fair_queue:
            args.append(f"cy:lockq:{{{key}}}")

        result = self.func_mgr.call_function("cy:locks", "acquire", keys, args)
        if result and len(result) >= 2:
            return {'acquired': result[0] == 1, 'fencing_token': result[1]}
        return {'acquired': False, 'fencing_token': 0}

    def release(self, key: str, owner: str, fair_queue: str = None):
        """Release a distributed lock"""
        keys = [f"cy:lock:{{{key}}}"]
        args = [owner]
        if fair_queue:
            args.append(f"cy:lockq:{{{key}}}")

        result = self.func_mgr.call_function("cy:locks", "release", keys, args)
        return result == 1

    def refresh(self, key: str, owner: str, ttl_ms: int = 30000):
        """Refresh/extend a lock"""
        keys = [f"cy:lock:{{{key}}}"]
        args = [owner, str(ttl_ms)]

        result = self.func_mgr.call_function("cy:locks", "refresh", keys, args)
        return result == 1

cdef class CyRateLimiter:
    """Rate limiting with multiple algorithms"""

    cdef CyRedisFunctionsManager func_mgr

    def __init__(self, func_mgr):
        self.func_mgr = func_mgr

    def token_bucket(self, key: str, capacity: int, refill_rate_per_ms: float, cost: int = 1):
        """Token bucket rate limiting"""
        now_ms = int(time.time() * 1000)
        keys = [f"cy:rate:{{{key}}}"]
        args = [str(now_ms), str(capacity), str(int(1000 / refill_rate_per_ms)), str(cost)]

        result = self.func_mgr.call_function("cy:rate", "token_bucket", keys, args)
        if result and len(result) >= 4:
            return {
                'allowed': result[0] == 1,
                'remaining': result[1],
                'reset_ms': result[2],
                'retry_after_ms': result[3]
            }
        return {'allowed': False, 'remaining': 0, 'retry_after_ms': 1000}

    def sliding_window(self, key: str, window_ms: int, max_requests: int):
        """Sliding window rate limiting"""
        now_ms = int(time.time() * 1000)
        keys = [f"cy:rate:{{{key}}}"]
        args = [str(now_ms), str(window_ms), str(max_requests)]

        result = self.func_mgr.call_function("cy:rate", "sliding_window", keys, args)
        if result and len(result) >= 3:
            return {
                'allowed': result[0] == 1,
                'remaining': result[1],
                'retry_after_ms': result[2]
            }
        return {'allowed': False, 'remaining': 0, 'retry_after_ms': 1000}

cdef class CyQueue:
    """Reliable key-based queues with deduplication"""

    cdef CyRedisFunctionsManager func_mgr

    def __init__(self, func_mgr):
        self.func_mgr = func_mgr

    def enqueue(self, name: str, message_id: str, payload: str,
                delay_ms: int = 0, ttl_ms: int = 0, priority: int = 0):
        """Enqueue a message"""
        args = [name, message_id, payload, str(delay_ms), str(ttl_ms), str(priority)]
        result = self.func_mgr.call_function("cy:queue", "enqueue", [], args)
        if result == 1:
            return "enqueued"
        elif result == 2:
            return "delayed"
        else:
            return "duplicate"

    def pull(self, name: str, visibility_ms: int = 30000, max_messages: int = 1):
        """Pull messages from queue"""
        now_ms = int(time.time() * 1000)
        args = [name, str(now_ms), str(visibility_ms), str(max_messages)]
        result = self.func_mgr.call_function("cy:queue", "pull", [], args)
        return result or []

    def ack(self, name: str, message_id: str):
        """Acknowledge message processing"""
        args = [name, message_id]
        result = self.func_mgr.call_function("cy:queue", "ack", [], args)
        return result == 1

    def nack(self, name: str, message_id: str, requeue: bool = True):
        """Negative acknowledge - return to queue or send to DLQ"""
        args = [name, message_id, "1" if requeue else "0"]
        result = self.func_mgr.call_function("cy:queue", "nack", [], args)
        return "requeued" if result == 1 else "dlq" if result == 2 else "not_found"

# Python wrapper
class RedisFunctions:
    """
    CyRedis Standard Library of Redis Functions
    High-level Python API for atomic Redis operations
    """

    def __init__(self, redis_client=None):
        if redis_client is None:
            from optimized_redis import OptimizedRedis
            redis_client = OptimizedRedis()

        self._func_mgr = CyRedisFunctionsManager(redis_client.client)
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
