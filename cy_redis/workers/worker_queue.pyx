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
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any

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

    def __cinit__(self, str queue_name, CyRedisClient redis_client,
                  int max_workers=4, str queue_type="default"):
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

    def start(self):
        """Start the worker queue"""
        if self.is_running:
            return

        self.is_running = True
        # Start worker threads
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop, args=(i,))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

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
        task_id = str(uuid.uuid4())
        task = {
            'id': task_id,
            'data': task_data,
            'created_at': time.time(),
            'delay': delay,
            'attempts': 0,
            'max_attempts': 3
        }

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
        """Main worker loop"""
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
            task_data, _ = tasks[0]
            task = json.loads(task_data)

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
        try:
            task['attempts'] += 1
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
