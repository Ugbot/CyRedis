-- CyRedis Lua Script: Advanced Job Queue
-- Implements job queuing with priorities, retries, and dead letter queues

local queue_key = KEYS[1]
local processing_key = KEYS[2]
local failed_key = KEYS[3]
local dead_letter_key = KEYS[4]
local operation = ARGV[1]
local current_time = tonumber(ARGV[2])

if operation == 'PUSH' then
    -- Push job to queue with priority and metadata
    local job_id = ARGV[3]
    local job_data = ARGV[4]
    local priority = tonumber(ARGV[5] or 0)
    local max_retries = tonumber(ARGV[6] or 3)
    local delay_seconds = tonumber(ARGV[7] or 0)

    local job = {
        id = job_id,
        data = job_data,
        priority = priority,
        max_retries = max_retries,
        retry_count = 0,
        created_at = current_time,
        delay_until = current_time + delay_seconds,
        status = 'queued'
    }

    local job_json = cjson.encode(job)

    if delay_seconds > 0 then
        -- Delayed job
        redis.call('ZADD', queue_key .. ':delayed', current_time + delay_seconds, job_json)
    else
        -- Immediate job with priority (lower score = higher priority)
        redis.call('ZADD', queue_key, priority, job_json)
    end

    return {pushed=true, job_id=job_id}

elseif operation == 'POP' then
    -- Pop job from queue, checking delayed jobs first
    local count = tonumber(ARGV[3] or 1)

    -- Move expired delayed jobs to main queue
    local delayed_jobs = redis.call('ZRANGEBYSCORE', queue_key .. ':delayed', 0, current_time, 'LIMIT', 0, count)
    if #delayed_jobs > 0 then
        for i, job_json in ipairs(delayed_jobs) do
            redis.call('ZADD', queue_key, 0, job_json)  -- Add to main queue with priority 0
        end
        redis.call('ZREMRANGEBYSCORE', queue_key .. ':delayed', 0, current_time)
    end

    -- Pop from main queue (highest priority first)
    local jobs = redis.call('ZRANGE', queue_key, 0, count - 1)

    if #jobs > 0 then
        -- Move to processing queue
        for i, job_json in ipairs(jobs) do
            redis.call('ZREM', queue_key, job_json)
            local processing_score = current_time + 300  -- 5 minute timeout
            redis.call('ZADD', processing_key, processing_score, job_json)
        end

        return {popped=jobs}
    else
        return {popped={}}
    end

elseif operation == 'COMPLETE' then
    -- Mark job as completed
    local job_id = ARGV[3]
    local processing_jobs = redis.call('ZRANGE', processing_key, 0, -1)

    for i, job_json in ipairs(processing_jobs) do
        local job = cjson.decode(job_json)
        if job.id == job_id then
            redis.call('ZREM', processing_key, job_json)
            return {completed=true, job_id=job_id}
        end
    end

    return {completed=false, error="Job not found in processing queue"}

elseif operation == 'FAIL' then
    -- Mark job as failed, potentially retry or move to dead letter queue
    local job_id = ARGV[3]
    local error_message = ARGV[4] or "Unknown error"
    local processing_jobs = redis.call('ZRANGE', processing_key, 0, -1)

    for i, job_json in ipairs(processing_jobs) do
        local job = cjson.decode(job_json)
        if job.id == job_id then
            redis.call('ZREM', processing_key, job_json)

            job.retry_count = job.retry_count + 1
            job.last_error = error_message
            job.failed_at = current_time

            if job.retry_count < job.max_retries then
                -- Retry with exponential backoff
                local backoff_seconds = math.pow(2, job.retry_count) * 60  -- 1min, 2min, 4min...
                job.status = 'retrying'
                local retry_job_json = cjson.encode(job)
                redis.call('ZADD', queue_key .. ':delayed', current_time + backoff_seconds, retry_job_json)

                return {retried=true, job_id=job_id, retry_count=job.retry_count, next_retry_in=backoff_seconds}
            else
                -- Move to dead letter queue
                job.status = 'dead'
                local dead_job_json = cjson.encode(job)
                redis.call('ZADD', dead_letter_key, current_time, dead_job_json)

                return {dead_letter=true, job_id=job_id, final_retry_count=job.retry_count}
            end
        end
    end

    return {failed=false, error="Job not found in processing queue"}

elseif operation == 'STATS' then
    -- Get queue statistics
    local queued = redis.call('ZCARD', queue_key)
    local delayed = redis.call('ZCARD', queue_key .. ':delayed')
    local processing = redis.call('ZCARD', processing_key)
    local failed = redis.call('ZCARD', failed_key)
    local dead = redis.call('ZCARD', dead_letter_key)

    return {
        queued = queued,
        delayed = delayed,
        processing = processing,
        failed = failed,
        dead_letter = dead,
        total = queued + delayed + processing + failed + dead
    }

end

return {error="Unknown operation: " .. operation}

