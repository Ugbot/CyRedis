# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Worker Queue System for CyRedis Web Application Support.
Provides distributed worker queue system for background task processing.
"""

import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

# Import core Redis functionality

from cy_redis.core.cy_redis_client cimport CyRedisClient


# Exception classes
class WorkerQueueError(Exception):
    pass


cdef class WorkerQueue:
    """
    Distributed worker queue system for background task processing.
    Supports multiple queue types and worker pools.
    """

    cdef readonly str queue_name
    cdef readonly str queue_type
    cdef object redis_client
    cdef int max_workers
    cdef str queue_key
    cdef str processing_key
    cdef str completed_key
    cdef str failed_key
    cdef bint is_running
    cdef object lock
    cdef object executor
    cdef list workers

    def __cinit__(self, str queue_name, CyRedisClient redis_client,
                  int max_workers=4, str queue_type="default"):
        # Preconditions: caller-supplied configuration must be sane. These are
        # caller/environment errors, so raise rather than assert.
        if not queue_name:
            raise WorkerQueueError("queue_name must be a non-empty string")
        if redis_client is None:
            raise WorkerQueueError("redis_client must not be None")
        if max_workers < 1:
            raise WorkerQueueError("max_workers must be >= 1")
        if not queue_type:
            raise WorkerQueueError("queue_type must be a non-empty string")

        self.queue_name = queue_name
        self.redis_client = redis_client
        self.max_workers = max_workers
        self.queue_type = queue_type
        self.workers = []
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.lock = threading.Lock()
        self.is_running = False

        # Queue keys
        self.queue_key = f"wq:{queue_type}:{queue_name}"
        self.processing_key = f"wq:processing:{queue_type}:{queue_name}"
        self.completed_key = f"wq:completed:{queue_type}:{queue_name}"
        self.failed_key = f"wq:failed:{queue_type}:{queue_name}"

        # Postconditions: invariants the rest of the object relies on.
        assert self.max_workers >= 1, "max_workers invariant violated"
        assert self.is_running is False, "queue must start in stopped state"
        assert len(self.workers) == 0, "worker list must start empty"

    def start(self):
        """Start the worker queue"""
        if self.is_running:
            return

        self.is_running = True
        # Start worker threads. Bounded by max_workers (validated >= 1 in
        # __cinit__), so this loop has a fixed, finite upper bound.
        assert self.max_workers >= 1, "max_workers must be positive before start"
        for worker_index in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop, args=(worker_index,))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

        # Postcondition: one thread spawned per configured worker slot.
        assert len(self.workers) == self.max_workers, "worker count mismatch after start"

    def stop(self):
        """Stop the worker queue"""
        self.is_running = False
        # Wait for workers to finish current tasks
        for worker in self.workers:
            worker.join(timeout=5.0)

        self.executor.shutdown(wait=True)

    def enqueue(self, task_data: Dict[str, Any], delay: int = 0) -> str:
        """
        Enqueue a task for processing.
        Returns task ID.
        """
        # Precondition: enqueue contract. delay is a non-negative second count.
        if delay < 0:
            raise WorkerQueueError("delay must be non-negative")

        task_id = str(uuid.uuid4())
        task = {
            'id': task_id,
            'data': task_data,
            'created_at': time.time(),
            'delay': delay,
            'attempts': 0,
            'max_attempts': 3
        }
        # Postcondition on the generated identity before it leaves the function.
        assert task_id, "generated task_id must be non-empty"
        assert task['attempts'] == 0, "new task must start with zero attempts"

        if delay > 0:
            # Schedule for future execution
            scheduled_time = time.time() + delay
            self.redis_client.zadd(f"wq:scheduled:{self.queue_type}:{self.queue_name}",
                                 {json.dumps(task): scheduled_time})
        else:
            # Add to immediate queue
            self.redis_client.lpush(self.queue_key, json.dumps(task))

        return task_id

    def _worker_loop(self, worker_id: int):
        """Main worker loop.

        Exit invariant: this loop runs until ``self.is_running`` is cleared by
        ``stop()`` (or by graceful shutdown elsewhere). ``is_running`` is the
        sole, documented termination condition; the loop is intentionally
        long-lived rather than bounded by an iteration count. Each iteration
        either blocks on a bounded ``brpoplpush`` (timeout=1) or sleeps on
        error, so it cannot busy-spin without yielding.
        """
        # Precondition: worker_id identifies a slot allocated in start().
        assert worker_id >= 0, "worker_id must be non-negative"
        while self.is_running:
            try:
                # Try to get a task from scheduled queue first
                scheduled_task = self._get_scheduled_task()
                if scheduled_task:
                    self._process_task(scheduled_task, worker_id)
                    continue

                # Get task from main queue
                task_data = self.redis_client.brpoplpush(
                    self.queue_key,
                    self.processing_key,
                    timeout=1
                )

                if task_data:
                    task = json.loads(task_data)
                    self._process_task(task, worker_id)

            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                time.sleep(1)

    def _get_scheduled_task(self) -> Optional[Dict[str, Any]]:
        """Get a task that is ready to be processed from scheduled queue"""
        current_time = time.time()
        tasks = self.redis_client.zrangebyscore(
            f"wq:scheduled:{self.queue_type}:{self.queue_name}",
            0, current_time, num=1, withscores=True
        )

        if tasks:
            assert len(tasks) == 1, "zrangebyscore num=1 must return at most one task"
            task_data, _ = tasks[0]
            task = json.loads(task_data)
            assert isinstance(task, dict), "scheduled task payload must decode to a dict"

            # Remove from scheduled queue
            self.redis_client.zrem(
                f"wq:scheduled:{self.queue_type}:{self.queue_name}",
                task_data
            )

            # Add to main queue for immediate processing
            self.redis_client.lpush(self.queue_key, task_data)
            return task

        return None

    def _process_task(self, task: Dict[str, Any], worker_id: int):
        """Process a single task"""
        # Preconditions: a task must carry an identity and a retry budget.
        assert isinstance(task, dict), "task must be a dict"
        assert 'id' in task, "task must carry an 'id'"
        assert 'max_attempts' in task, "task must carry a 'max_attempts' budget"
        assert worker_id >= 0, "worker_id must be non-negative"
        try:
            task['attempts'] += 1
            assert task['attempts'] >= 1, "attempts must increment to >= 1"
            task['started_at'] = time.time()
            task['worker_id'] = worker_id

            # Update processing status
            self.redis_client.hset(
                self.processing_key,
                task['id'],
                json.dumps(task)
            )

            # Process the task (this would be overridden by subclasses)
            result = self.process_task(task)

            # Mark as completed
            task['completed_at'] = time.time()
            task['result'] = result

            self.redis_client.lpush(self.completed_key, json.dumps(task))
            self.redis_client.hdel(self.processing_key, task['id'])

        except Exception as e:
            task['failed_at'] = time.time()
            task['error'] = str(e)

            if task['attempts'] < task['max_attempts']:
                # Re-queue for retry
                retry_delay = min(60, 2 ** task['attempts'])  # Exponential backoff
                task['retry_at'] = time.time() + retry_delay
                self.redis_client.lpush(self.queue_key, json.dumps(task))
            else:
                # Mark as failed
                self.redis_client.lpush(self.failed_key, json.dumps(task))

            self.redis_client.hdel(self.processing_key, task['id'])

    def process_task(self, task: Dict[str, Any]) -> Any:
        """
        Process a task. Override this method in subclasses.
        Default implementation just returns the task data.
        """
        return task['data']

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return {
            'queue_length': self.redis_client.llen(self.queue_key),
            'processing_count': self.redis_client.hlen(self.processing_key),
            'completed_count': self.redis_client.llen(self.completed_key),
            'failed_count': self.redis_client.llen(self.failed_key),
            'scheduled_count': self.redis_client.zcard(f"wq:scheduled:{self.queue_type}:{self.queue_name}")
        }
