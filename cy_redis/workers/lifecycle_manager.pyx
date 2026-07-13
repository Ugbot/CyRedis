# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Lifecycle Manager for CyRedis Web Application Support.
Provides enhanced lifecycle hooks for startup/shutdown events with worker coordination.
"""

import json
import os
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional

# Import core Redis functionality

from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class LifecycleManager:
    """
    Enhanced lifecycle hooks for startup/shutdown events with worker coordination.
    Manages initialization, cleanup, and graceful shutdown with workload yielding.
    """

    cdef object redis_client
    cdef public list startup_hooks
    cdef public list shutdown_hooks
    cdef bint is_initialized
    cdef public str worker_id
    cdef str worker_status_key
    cdef str workload_transfer_key
    cdef str heartbeat_key

    def __cinit__(self, object redis_client, str worker_id=None):
        # Precondition: a Redis client is mandatory for coordination.
        if redis_client is None:
            raise ValueError("redis_client must not be None")

        self.redis_client = redis_client
        self.startup_hooks = []
        self.shutdown_hooks = []
        self.is_initialized = False
        self.worker_id = worker_id or self._generate_worker_id()

        # Worker coordination keys
        self.worker_status_key = "workers:status"
        self.workload_transfer_key = "workers:workload_transfer"
        self.heartbeat_key = f"workers:heartbeat:{self.worker_id}"

        # Postcondition: identity and keys derived before registration.
        assert self.worker_id, "worker_id must be non-empty"
        assert self.heartbeat_key.endswith(self.worker_id), "heartbeat key must embed worker_id"

        # Register this worker
        self._register_worker()

    def _generate_worker_id(self) -> str:
        """Generate unique worker ID"""
        pid = os.getpid()
        hostname = socket.gethostname()
        timestamp = int(time.time() * 1000)
        # Postconditions: identity components are well-formed.
        assert pid > 0, "process id must be positive"
        assert hostname, "hostname must be non-empty"
        worker_id = f"worker_{hostname}_{pid}_{timestamp}"
        assert worker_id.startswith("worker_"), "worker_id must carry the worker_ prefix"
        return worker_id

    def _register_worker(self):
        """Register this worker in the cluster"""
        worker_info = {
            'worker_id': self.worker_id,
            'hostname': socket.gethostname(),
            'pid': os.getpid(),
            'status': 'starting',
            'started_at': time.time(),
            'last_heartbeat': time.time()
        }

        self.redis_client.hset(self.worker_status_key, self.worker_id, json.dumps(worker_info))

    def _update_heartbeat(self):
        """Update worker heartbeat in both the status entry and the dedicated
        heartbeat key, so health checks (which read the status entry) see a
        fresh timestamp."""
        now = time.time()
        raw = self.redis_client.hget(self.worker_status_key, self.worker_id)
        try:
            info = json.loads(raw) if raw else {'worker_id': self.worker_id}
        except (ValueError, TypeError):
            info = {'worker_id': self.worker_id}
        info['last_heartbeat'] = now
        self.redis_client.hset(self.worker_status_key, self.worker_id, json.dumps(info))
        self.redis_client.hset(self.heartbeat_key, 'last_heartbeat', now)

    def _mark_worker_dead(self):
        """Mark worker as dead"""
        worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
        if worker_info:
            worker_info['status'] = 'dead'
            worker_info['died_at'] = time.time()
            self.redis_client.hset(self.worker_status_key, self.worker_id, json.dumps(worker_info))

    def add_startup_hook(self, hook_func: Callable, priority: int = 0):
        """Add a startup hook function with priority"""
        if not callable(hook_func):
            raise ValueError("hook_func must be callable")
        hooks_before = len(self.startup_hooks)
        self.startup_hooks.append((priority, hook_func))
        # Sort by priority (lower number = higher priority)
        self.startup_hooks.sort(key=lambda x: x[0])
        # Postcondition: exactly one hook added.
        assert len(self.startup_hooks) == hooks_before + 1, "startup hook not appended"

    def add_shutdown_hook(self, hook_func: Callable, priority: int = 0):
        """Add a shutdown hook function with priority"""
        if not callable(hook_func):
            raise ValueError("hook_func must be callable")
        hooks_before = len(self.shutdown_hooks)
        self.shutdown_hooks.append((priority, hook_func))
        # Sort by priority (lower number = higher priority)
        self.shutdown_hooks.sort(key=lambda x: x[0])
        # Postcondition: exactly one hook added.
        assert len(self.shutdown_hooks) == hooks_before + 1, "shutdown hook not appended"

    def initialize(self):
        """Run all startup hooks in priority order"""
        if self.is_initialized:
            return

        print(f"Initializing worker {self.worker_id}")

        # Update status
        self._update_worker_status('initializing')

        try:
            # Run startup hooks in priority order
            for priority, hook in self.startup_hooks:
                try:
                    hook()
                except Exception as e:
                    print(f"Startup hook (priority {priority}) error: {e}")

            self.is_initialized = True
            self._update_worker_status('running')
            print(f"Worker {self.worker_id} initialized successfully")

        except Exception as e:
            print(f"Worker {self.worker_id} initialization failed: {e}")
            self._update_worker_status('failed')
            raise

    def shutdown(self, graceful: bool = True):
        """Run all shutdown hooks with graceful workload yielding"""
        if not self.is_initialized:
            return

        print(f"Shutting down worker {self.worker_id} (graceful={graceful})")

        if graceful:
            # Graceful shutdown process
            self._graceful_shutdown()
        else:
            # Immediate shutdown
            self._immediate_shutdown()

    def _graceful_shutdown(self):
        """Perform graceful shutdown with workload yielding"""
        # Update status
        self._update_worker_status('shutting_down')

        try:
            # 1. Yield current workload to other workers
            self._yield_workload()

            # 2. Wait for active tasks to complete (configurable timeout)
            self._wait_for_tasks(timeout=30)

            # 3. Transfer session state if needed
            self._transfer_session_state()

            # 4. Run shutdown hooks in reverse priority order (cleanup first)
            for priority, hook in reversed(self.shutdown_hooks):
                try:
                    hook()
                except Exception as e:
                    print(f"Shutdown hook (priority {priority}) error: {e}")

            # 5. Mark worker as stopped
            self._update_worker_status('stopped')

        except Exception as e:
            print(f"Graceful shutdown error: {e}")
            # Fall back to immediate shutdown
            self._immediate_shutdown()

    def _immediate_shutdown(self):
        """Perform immediate shutdown without waiting"""
        try:
            # Run shutdown hooks in reverse priority order
            for priority, hook in reversed(self.shutdown_hooks):
                try:
                    hook()
                except Exception as e:
                    print(f"Shutdown hook (priority {priority}) error: {e}")

            # Mark worker as dead
            self._mark_worker_dead()

        finally:
            self.is_initialized = False
            print(f"Worker {self.worker_id} shutdown complete")

    def _yield_workload(self):
        """Yield current workload to other available workers"""
        try:
            # Get current worker's active tasks/sessions
            active_workload = self._get_active_workload()

            if active_workload:
                # Find available workers
                available_workers = self._get_available_workers()

                if available_workers:
                    # Distribute workload to other workers
                    self._distribute_workload(active_workload, available_workers)
                    print(f"Yielded {len(active_workload)} workload items to other workers")
                else:
                    print("No available workers to yield workload to")

        except Exception as e:
            print(f"Workload yielding error: {e}")

    def _get_active_workload(self) -> Dict[str, Any]:
        """Get current worker's active workload"""
        workload = {}

        try:
            # Get active sessions for this worker
            active_sessions = self.redis_client.smembers(f"sessions:worker:{self.worker_id}")
            workload['sessions'] = list(active_sessions)

            # Get pending tasks for this worker
            pending_tasks = self.redis_client.llen(f"tasks:worker:{self.worker_id}")
            workload['pending_tasks'] = pending_tasks

            # Get in-progress tasks
            processing_tasks = self.redis_client.hlen(f"tasks:processing:{self.worker_id}")
            workload['processing_tasks'] = processing_tasks

        except Exception as e:
            print(f"Error getting active workload: {e}")

        return workload

    def _get_available_workers(self) -> List[str]:
        """Get list of available workers"""
        # A peer is available if running and seen within this many seconds.
        available_heartbeat_timeout_seconds = 60
        assert available_heartbeat_timeout_seconds > 0, "heartbeat timeout must be positive"
        try:
            workers = self.redis_client.hgetall(self.worker_status_key)
            available = []

            # Bound: one iteration per registered worker.
            for worker_id, worker_info_str in workers.items():
                worker_info = json.loads(worker_info_str)
                seconds_since_heartbeat = time.time() - worker_info.get('last_heartbeat', 0)
                is_other_worker = worker_id != self.worker_id
                is_running = worker_info.get('status') == 'running'
                is_recent = seconds_since_heartbeat < available_heartbeat_timeout_seconds
                if is_other_worker and is_running and is_recent:
                    available.append(worker_id)

            # Postcondition: this worker never appears in its own peer list.
            assert self.worker_id not in available, "self must be excluded from peers"
            return available

        except Exception as e:
            print(f"Error getting available workers: {e}")
            return []

    def _distribute_workload(self, workload: Dict[str, Any], available_workers: List[str]):
        """Distribute workload to available workers"""
        # Preconditions: callers (_yield_workload) only invoke this with a
        # non-empty worker list; the divisor below depends on it.
        assert isinstance(workload, dict), "workload must be a dict"
        if not available_workers:
            raise ValueError("available_workers must not be empty")
        try:
            # Distribute sessions
            if workload.get('sessions'):
                assert len(available_workers) >= 1, "divisor must be positive"
                sessions_per_worker = len(workload['sessions']) // len(available_workers)
                # Bound: one iteration per available worker.
                for i, worker_id in enumerate(available_workers):
                    start_idx = i * sessions_per_worker
                    end_idx = start_idx + sessions_per_worker if i < len(available_workers) - 1 else len(workload['sessions'])
                    worker_sessions = workload['sessions'][start_idx:end_idx]

                    for session_id in worker_sessions:
                        # Transfer session ownership
                        self.redis_client.hset(f"session:{session_id}", 'worker_id', worker_id)

                    print(f"Transferred {len(worker_sessions)} sessions to worker {worker_id}")

            # Mark tasks for redistribution
            if workload.get('processing_tasks', 0) > 0:
                # Move processing tasks back to main queue for redistribution
                processing_key = f"tasks:processing:{self.worker_id}"
                tasks = self.redis_client.hgetall(processing_key)

                for task_id, task_data in tasks.items():
                    # Add back to main queue
                    self.redis_client.lpush("tasks:main_queue", task_data)
                    # Remove from processing
                    self.redis_client.hdel(processing_key, task_id)

                print(f"Redistributed {len(tasks)} processing tasks")

        except Exception as e:
            print(f"Error distributing workload: {e}")

    def _wait_for_tasks(self, timeout: int = 30):
        """Wait for active tasks to complete.

        Exit invariant: the loop terminates either when no active work remains
        or once ``timeout`` seconds have elapsed. With a 2-second sleep per
        iteration, the wall-clock timeout bounds the iteration count, so the
        loop is provably finite.
        """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        start_time = time.time()

        while time.time() - start_time < timeout:
            active_tasks = self._get_active_workload()

            # Check if we still have active work
            active_session_count = len(active_tasks.get('sessions', []))
            active_processing_count = active_tasks.get('processing_tasks', 0)
            has_work = active_session_count > 0 or active_processing_count > 0

            if not has_work:
                print("All active tasks completed")
                break

            print(f"Waiting for {active_session_count} sessions and {active_processing_count} tasks to complete...")
            time.sleep(2)

        # Postcondition: we never wait beyond a small slack over the timeout.
        elapsed_seconds = time.time() - start_time
        assert elapsed_seconds >= 0, "elapsed time must be non-negative"
        if elapsed_seconds >= timeout:
            print(f"Shutdown timeout reached after {timeout} seconds")

    def _transfer_session_state(self):
        """Transfer session state to other workers if needed"""
        # This would handle transferring in-memory session state
        # For now, we rely on Redis persistence
        pass

    def _update_worker_status(self, status: str):
        """Update worker status in Redis"""
        if not status:
            raise ValueError("status must be a non-empty string")
        try:
            worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
            worker_info['status'] = status
            worker_info['last_heartbeat'] = time.time()
            # Postcondition: the in-memory record reflects the requested status.
            assert worker_info['status'] == status, "status not applied before persist"
            self.redis_client.hset(self.worker_status_key, self.worker_id, json.dumps(worker_info))
        except Exception as e:
            print(f"Error updating worker status: {e}")

    def get_worker_stats(self) -> Dict[str, Any]:
        """Get statistics for this worker"""
        try:
            worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
            uptime = time.time() - worker_info.get('started_at', time.time())

            return {
                'worker_id': self.worker_id,
                'status': worker_info.get('status', 'unknown'),
                'uptime_seconds': uptime,
                'hostname': worker_info.get('hostname', 'unknown'),
                'pid': worker_info.get('pid', 0)
            }

        except Exception as e:
            print(f"Error getting worker stats: {e}")
            return {'error': str(e)}

    def is_healthy(self) -> bool:
        """Check if worker is healthy"""
        healthy_heartbeat_timeout_seconds = 60  # 60 second timeout
        assert healthy_heartbeat_timeout_seconds > 0, "heartbeat timeout must be positive"
        try:
            worker_info = json.loads(self.redis_client.hget(self.worker_status_key, self.worker_id) or '{}')
            last_heartbeat = worker_info.get('last_heartbeat', 0)
            seconds_since_heartbeat = time.time() - last_heartbeat
            return seconds_since_heartbeat < healthy_heartbeat_timeout_seconds
        except Exception:
            return False
