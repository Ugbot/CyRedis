# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Worker Coordinator for CyRedis Web Application Support.
Coordinates multiple workers for graceful scaling and recovery.
"""

import json
import time
from typing import Dict, List, Any

# Import core Redis functionality
from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class WorkerCoordinator:
    """
    Coordinates multiple workers for graceful scaling and recovery.
    Handles worker registration, health monitoring, and workload distribution.
    """

    cdef object redis_client
    cdef readonly str coordinator_id
    cdef str worker_status_key
    cdef str dead_workers_key
    cdef str workload_distribution_key

    def __cinit__(self, object redis_client, str coordinator_id=None):
        # Precondition: a coordinator is useless without a Redis client.
        if redis_client is None:
            raise ValueError("redis_client must not be None")

        self.redis_client = redis_client
        self.coordinator_id = coordinator_id or f"coordinator_{int(time.time())}"
        self.worker_status_key = "workers:status"
        self.dead_workers_key = "workers:dead"
        self.workload_distribution_key = "workers:workload_distribution"

        # Postcondition: identity and key namespaces are established.
        assert self.coordinator_id, "coordinator_id must be non-empty"
        assert self.worker_status_key, "worker_status_key must be non-empty"

    def register_worker(self, worker_id: str, worker_info: Dict[str, Any]):
        """Register a new worker"""
        # Preconditions: identity + payload required (caller error -> raise).
        if not worker_id:
            raise ValueError("worker_id must be a non-empty string")
        if worker_info is None:
            raise ValueError("worker_info must not be None")
        assert isinstance(worker_info, dict), "worker_info must be a dict"

        worker_info['registered_at'] = time.time()
        worker_info['coordinator_id'] = self.coordinator_id

        # Postcondition: the worker is tagged with this coordinator's identity.
        assert worker_info['coordinator_id'] == self.coordinator_id, "coordinator tag mismatch"
        self.redis_client.hset(self.worker_status_key, worker_id, json.dumps(worker_info))

    def unregister_worker(self, worker_id: str):
        """Unregister a worker"""
        if not worker_id:
            raise ValueError("worker_id must be a non-empty string")
        self.redis_client.hdel(self.worker_status_key, worker_id)

    def get_all_workers(self) -> Dict[str, Dict[str, Any]]:
        """Get all registered workers"""
        workers = self.redis_client.hgetall(self.worker_status_key)
        return {worker_id: json.loads(worker_info) for worker_id, worker_info in workers.items()}

    def get_healthy_workers(self) -> List[str]:
        """Get list of healthy workers"""
        # A worker is healthy if running and seen within this many seconds.
        healthy_heartbeat_timeout_seconds = 60
        assert healthy_heartbeat_timeout_seconds > 0, "heartbeat timeout must be positive"

        workers = self.get_all_workers()
        healthy = []

        # Bound: one iteration per registered worker.
        for worker_id, worker_info in workers.items():
            seconds_since_heartbeat = time.time() - worker_info.get('last_heartbeat', 0)
            is_running = worker_info.get('status') == 'running'
            is_recent = seconds_since_heartbeat < healthy_heartbeat_timeout_seconds
            if is_running and is_recent:
                healthy.append(worker_id)

        # Postcondition: healthy is a subset of all known workers.
        assert len(healthy) <= len(workers), "healthy count exceeds total workers"
        return healthy

    def detect_dead_workers(self) -> List[str]:
        """Detect workers that have stopped responding"""
        # A non-stopped worker is dead once unseen for longer than this.
        dead_heartbeat_timeout_seconds = 120  # 2 minute timeout
        assert dead_heartbeat_timeout_seconds > 0, "dead timeout must be positive"

        workers = self.get_all_workers()
        dead = []

        # Bound: one iteration per registered worker.
        for worker_id, worker_info in workers.items():
            seconds_since_heartbeat = time.time() - worker_info.get('last_heartbeat', 0)
            is_not_stopped = worker_info.get('status') != 'stopped'
            is_stale = seconds_since_heartbeat > dead_heartbeat_timeout_seconds
            if is_not_stopped and is_stale:
                dead.append(worker_id)

        # Postcondition: dead is a subset of all known workers.
        assert len(dead) <= len(workers), "dead count exceeds total workers"
        return dead

    def handle_dead_worker(self, worker_id: str):
        """Handle a dead worker by redistributing its workload"""
        if not worker_id:
            raise ValueError("worker_id must be a non-empty string")
        print(f"Handling dead worker: {worker_id}")

        try:
            # Mark worker as dead
            worker_info = self.redis_client.hget(self.worker_status_key, worker_id)
            if worker_info:
                worker_info = json.loads(worker_info)
                worker_info['status'] = 'dead'
                worker_info['died_at'] = time.time()
                self.redis_client.hset(self.worker_status_key, worker_id, json.dumps(worker_info))

            # Redistribute workload
            self._redistribute_dead_worker_workload(worker_id)

        except Exception as e:
            print(f"Error handling dead worker {worker_id}: {e}")

    def _redistribute_dead_worker_workload(self, worker_id: str):
        """Redistribute workload from dead worker"""
        assert worker_id, "worker_id must be non-empty"
        try:
            # Find sessions owned by dead worker
            dead_worker_sessions = self.redis_client.smembers(f"sessions:worker:{worker_id}")

            if dead_worker_sessions:
                # Find healthy workers to take over sessions
                healthy_workers = self.get_healthy_workers()
                if healthy_workers:
                    # Distribute sessions among healthy workers. Guarded by the
                    # enclosing `if healthy_workers`, so the divisor is >= 1.
                    assert len(healthy_workers) >= 1, "divisor must be positive"
                    sessions_per_worker = len(dead_worker_sessions) // len(healthy_workers)
                    assert sessions_per_worker >= 0, "sessions_per_worker must be non-negative"

                    # Bound: one iteration per healthy worker.
                    for i, healthy_worker_id in enumerate(healthy_workers):
                        start_idx = i * sessions_per_worker
                        end_idx = start_idx + sessions_per_worker if i < len(healthy_workers) - 1 else len(dead_worker_sessions)
                        worker_sessions = list(dead_worker_sessions)[start_idx:end_idx]

                        for session_id in worker_sessions:
                            # Transfer session ownership
                            self.redis_client.hset(f"session:{session_id}", 'worker_id', healthy_worker_id)

                        print(f"Redistributed {len(worker_sessions)} sessions to worker {healthy_worker_id}")

            # Handle dead worker's processing tasks
            processing_key = f"tasks:processing:{worker_id}"
            dead_tasks = self.redis_client.hgetall(processing_key)

            if dead_tasks:
                for task_id, task_data in dead_tasks.items():
                    # Add back to main queue for redistribution
                    self.redis_client.lpush("tasks:main_queue", task_data)

                print(f"Redistributed {len(dead_tasks)} tasks from dead worker")

        except Exception as e:
            print(f"Error redistributing dead worker workload: {e}")

    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get cluster-wide statistics"""
        workers = self.get_all_workers()

        stats = {
            'total_workers': len(workers),
            'healthy_workers': len(self.get_healthy_workers()),
            'dead_workers': len(self.detect_dead_workers()),
            'workers_by_status': {}
        }

        # Count workers by status
        for worker_info in workers.values():
            status = worker_info.get('status', 'unknown')
            stats['workers_by_status'][status] = stats['workers_by_status'].get(status, 0) + 1

        return stats
