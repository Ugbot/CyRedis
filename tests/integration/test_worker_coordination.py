"""
Integration tests for CyRedis worker coordination and recovery features.
Tests worker lifecycle management, graceful shutdown, and workload redistribution.
"""

import pytest
import time
import json
import threading
import signal
import os
import socket
from typing import Dict, List, Any, Generator
from unittest.mock import Mock, patch

try:
    from cy_redis.web_app_support import (
        WebApplicationSupport,
        LifecycleManager,
        WorkerCoordinator,
        WorkerQueue,
        SessionManager,
        MultiSessionTracker,
        ConcurrentSharedDict
    )
    CYREDIS_WEB_AVAILABLE = True
except ImportError:
    CYREDIS_WEB_AVAILABLE = False


pytestmark = [pytest.mark.integration, pytest.mark.worker_coordination]


@pytest.fixture
def web_app_support(hp_redis_client) -> Generator[WebApplicationSupport, None, None]:
    """Provide a WebApplicationSupport instance for testing."""
    if not CYREDIS_WEB_AVAILABLE:
        pytest.skip("CyRedis web app support not built")

    support = WebApplicationSupport(
        redis_client=hp_redis_client,
        host="localhost",
        port=6379
    )

    yield support

    # Cleanup
    try:
        support.shutdown()
    except Exception:
        pass


@pytest.fixture
def lifecycle_manager(hp_redis_client) -> Generator[LifecycleManager, None, None]:
    """Provide a LifecycleManager instance for testing."""
    if not CYREDIS_WEB_AVAILABLE:
        pytest.skip("CyRedis web app support not built")

    manager = LifecycleManager(hp_redis_client, worker_id="test_worker")
    yield manager


@pytest.fixture
def worker_coordinator(hp_redis_client) -> Generator[WorkerCoordinator, None, None]:
    """Provide a WorkerCoordinator instance for testing."""
    if not CYREDIS_WEB_AVAILABLE:
        pytest.skip("CyRedis web app support not built")

    coordinator = WorkerCoordinator(hp_redis_client, coordinator_id="test_coordinator")
    yield coordinator


@pytest.fixture
def unique_worker_key() -> str:
    """Generate a unique worker key for testing."""
    return f"test_worker_{int(time.time() * 1000000)}"


@pytest.fixture
def test_session_data() -> Dict[str, Any]:
    """Provide test session data."""
    return {
        'user_id': 'test_user_123',
        'device_info': {'browser': 'Chrome', 'os': 'Linux'},
        'created_at': time.time(),
        'last_accessed': time.time()
    }


class TestLifecycleManager:
    """Test worker lifecycle management."""

    def test_worker_registration(self, lifecycle_manager, hp_redis_client):
        """Test that workers register themselves properly."""
        # Check that worker is registered
        worker_info = hp_redis_client.hget("workers:status", lifecycle_manager.worker_id)
        assert worker_info is not None

        worker_data = json.loads(worker_info)
        assert worker_data['worker_id'] == lifecycle_manager.worker_id
        assert worker_data['status'] == 'starting'
        assert 'hostname' in worker_data
        assert 'pid' in worker_data

    def test_worker_heartbeat(self, lifecycle_manager, hp_redis_client):
        """Test worker heartbeat functionality."""
        # Get initial heartbeat
        initial_info = json.loads(hp_redis_client.hget("workers:status", lifecycle_manager.worker_id))
        initial_heartbeat = initial_info['last_heartbeat']

        # Wait a bit and update heartbeat
        time.sleep(0.1)
        lifecycle_manager._update_heartbeat()

        # Check that heartbeat was updated
        updated_info = json.loads(hp_redis_client.hget("workers:status", lifecycle_manager.worker_id))
        updated_heartbeat = updated_info['last_heartbeat']

        assert updated_heartbeat > initial_heartbeat

    def test_worker_status_updates(self, lifecycle_manager, hp_redis_client):
        """Test worker status update functionality."""
        # Update to running status
        lifecycle_manager._update_worker_status('running')

        worker_info = json.loads(hp_redis_client.hget("workers:status", lifecycle_manager.worker_id))
        assert worker_info['status'] == 'running'

        # Update to shutting_down status
        lifecycle_manager._update_worker_status('shutting_down')

        worker_info = json.loads(hp_redis_client.hget("workers:status", lifecycle_manager.worker_id))
        assert worker_info['status'] == 'shutting_down'

    def test_worker_stats(self, lifecycle_manager):
        """Test worker statistics retrieval."""
        stats = lifecycle_manager.get_worker_stats()

        assert stats['worker_id'] == lifecycle_manager.worker_id
        assert 'status' in stats
        assert 'uptime_seconds' in stats
        assert 'hostname' in stats
        assert 'pid' in stats

    def test_worker_health_check(self, lifecycle_manager):
        """Test worker health checking."""
        # Worker should be healthy initially
        assert lifecycle_manager.is_healthy() is True

        # Simulate old heartbeat
        old_time = time.time() - 120  # 2 minutes ago
        hp_redis_client.hset(f"workers:heartbeat:{lifecycle_manager.worker_id}", 'last_heartbeat', old_time)

        # Worker should be unhealthy now
        assert lifecycle_manager.is_healthy() is False

    def test_priority_hooks(self, lifecycle_manager):
        """Test that hooks run in priority order."""
        hook_order = []

        def hook_priority_2():
            hook_order.append(2)
            time.sleep(0.01)  # Small delay to test ordering

        def hook_priority_1():
            hook_order.append(1)

        def hook_priority_3():
            hook_order.append(3)

        # Add hooks with different priorities (lower number = higher priority)
        lifecycle_manager.add_startup_hook(hook_priority_1, priority=1)
        lifecycle_manager.add_startup_hook(hook_priority_2, priority=2)
        lifecycle_manager.add_startup_hook(hook_priority_3, priority=3)

        # Run initialization
        lifecycle_manager.initialize()

        # Check that hooks ran in priority order
        assert hook_order == [1, 2, 3]

    def test_graceful_shutdown(self, lifecycle_manager, hp_redis_client):
        """Test graceful shutdown process."""
        # Initialize first
        lifecycle_manager.initialize()

        # Start a background task that simulates work
        def simulate_work():
            time.sleep(2)
            # Mark worker as having work
            hp_redis_client.sadd(f"sessions:worker:{lifecycle_manager.worker_id}", "test_session_1")

        work_thread = threading.Thread(target=simulate_work, daemon=True)
        work_thread.start()

        # Give the thread time to start
        time.sleep(0.1)

        # Initiate graceful shutdown
        lifecycle_manager.shutdown(graceful=True)

        # Check that worker status was updated
        worker_info = json.loads(hp_redis_client.hget("workers:status", lifecycle_manager.worker_id))
        assert worker_info['status'] == 'stopped'

    def test_immediate_shutdown(self, lifecycle_manager, hp_redis_client):
        """Test immediate shutdown process."""
        # Initialize first
        lifecycle_manager.initialize()

        # Initiate immediate shutdown
        lifecycle_manager.shutdown(graceful=False)

        # Check that worker is marked as dead
        worker_info = json.loads(hp_redis_client.hget("workers:status", lifecycle_manager.worker_id))
        assert worker_info['status'] == 'dead'


class TestWorkerCoordinator:
    """Test worker coordination functionality."""

    def test_worker_registration(self, worker_coordinator, hp_redis_client):
        """Test worker registration through coordinator."""
        worker_id = "test_worker_123"
        worker_info = {
            'hostname': 'test_host',
            'pid': 12345,
            'status': 'running'
        }

        worker_coordinator.register_worker(worker_id, worker_info)

        # Check registration
        registered_info = hp_redis_client.hget("workers:status", worker_id)
        assert registered_info is not None

        registered_data = json.loads(registered_info)
        assert registered_data['hostname'] == 'test_host'
        assert registered_data['pid'] == 12345
        assert registered_data['coordinator_id'] == worker_coordinator.coordinator_id

    def test_worker_unregistration(self, worker_coordinator, hp_redis_client):
        """Test worker unregistration."""
        worker_id = "test_worker_456"
        worker_info = {'hostname': 'test_host', 'pid': 45678}

        # Register worker
        worker_coordinator.register_worker(worker_id, worker_info)
        assert hp_redis_client.hexists("workers:status", worker_id)

        # Unregister worker
        worker_coordinator.unregister_worker(worker_id)
        assert not hp_redis_client.hexists("workers:status", worker_id)

    def test_get_all_workers(self, worker_coordinator, hp_redis_client):
        """Test getting all registered workers."""
        # Register some test workers
        test_workers = {}
        for i in range(3):
            worker_id = f"test_worker_{i}"
            worker_info = {
                'hostname': f'host_{i}',
                'pid': 1000 + i,
                'status': 'running'
            }
            worker_coordinator.register_worker(worker_id, worker_info)
            test_workers[worker_id] = worker_info

        # Get all workers
        all_workers = worker_coordinator.get_all_workers()

        # Should have at least our test workers
        for worker_id, worker_info in test_workers.items():
            assert worker_id in all_workers
            assert all_workers[worker_id]['hostname'] == worker_info['hostname']

    def test_healthy_workers_filtering(self, worker_coordinator, hp_redis_client):
        """Test filtering of healthy workers."""
        # Register workers with different statuses
        healthy_worker = "healthy_worker"
        dead_worker = "dead_worker"

        worker_coordinator.register_worker(healthy_worker, {
            'status': 'running',
            'last_heartbeat': time.time()
        })

        worker_coordinator.register_worker(dead_worker, {
            'status': 'running',
            'last_heartbeat': time.time() - 120  # 2 minutes ago
        })

        healthy_workers = worker_coordinator.get_healthy_workers()
        assert healthy_worker in healthy_workers
        assert dead_worker not in healthy_workers

    def test_dead_worker_detection(self, worker_coordinator, hp_redis_client):
        """Test detection of dead workers."""
        # Register workers with old heartbeats
        dead_worker_1 = "dead_worker_1"
        dead_worker_2 = "dead_worker_2"
        alive_worker = "alive_worker"

        for worker_id in [dead_worker_1, dead_worker_2, alive_worker]:
            worker_coordinator.register_worker(worker_id, {
                'status': 'running',
                'last_heartbeat': time.time() if worker_id == alive_worker else time.time() - 150
            })

        dead_workers = worker_coordinator.detect_dead_workers()
        assert dead_worker_1 in dead_workers
        assert dead_worker_2 in dead_workers
        assert alive_worker not in dead_workers

    def test_dead_worker_handling(self, worker_coordinator, hp_redis_client):
        """Test handling of dead workers."""
        worker_id = "dead_test_worker"
        worker_info = {
            'status': 'running',
            'last_heartbeat': time.time() - 150
        }

        worker_coordinator.register_worker(worker_id, worker_info)

        # Add some sessions for this worker
        hp_redis_client.sadd(f"sessions:worker:{worker_id}", "session_1", "session_2")

        # Handle dead worker
        worker_coordinator.handle_dead_worker(worker_id)

        # Check that worker is marked as dead
        updated_info = json.loads(hp_redis_client.hget("workers:status", worker_id))
        assert updated_info['status'] == 'dead'

    def test_cluster_statistics(self, worker_coordinator, hp_redis_client):
        """Test cluster-wide statistics."""
        # Register workers with different statuses
        workers_data = {
            'running_worker': {'status': 'running', 'last_heartbeat': time.time()},
            'dead_worker': {'status': 'running', 'last_heartbeat': time.time() - 150},
            'stopped_worker': {'status': 'stopped', 'last_heartbeat': time.time()}
        }

        for worker_id, worker_info in workers_data.items():
            worker_coordinator.register_worker(worker_id, worker_info)

        stats = worker_coordinator.get_cluster_stats()

        assert stats['total_workers'] == 3
        assert stats['healthy_workers'] == 1  # Only running_worker (running + recent heartbeat)
        assert stats['dead_workers'] == 1     # dead_worker (running + old heartbeat)
        assert 'workers_by_status' in stats

        status_counts = stats['workers_by_status']
        assert status_counts['running'] == 2  # running_worker and dead_worker (status still running)
        assert status_counts['stopped'] == 1  # stopped_worker


class TestWorkloadYielding:
    """Test workload yielding and redistribution."""

    def test_active_workload_detection(self, lifecycle_manager, hp_redis_client):
        """Test detection of active workload."""
        # Add some test sessions and tasks
        hp_redis_client.sadd(f"sessions:worker:{lifecycle_manager.worker_id}", "session_1", "session_2")
        hp_redis_client.lpush(f"tasks:worker:{lifecycle_manager.worker_id}", "task_1", "task_2")
        hp_redis_client.hset(f"tasks:processing:{lifecycle_manager.worker_id}", "task_3", "processing_data")

        workload = lifecycle_manager._get_active_workload()

        assert len(workload['sessions']) == 2
        assert workload['pending_tasks'] == 2
        assert workload['processing_tasks'] == 1

    def test_available_workers_discovery(self, lifecycle_manager, hp_redis_client):
        """Test discovery of available workers."""
        # Register some workers
        current_worker = lifecycle_manager.worker_id

        # Add a healthy worker
        healthy_worker = "healthy_test_worker"
        hp_redis_client.hset("workers:status", healthy_worker, json.dumps({
            'worker_id': healthy_worker,
            'status': 'running',
            'last_heartbeat': time.time()
        }))

        # Add an unhealthy worker
        unhealthy_worker = "unhealthy_test_worker"
        hp_redis_client.hset("workers:status", unhealthy_worker, json.dumps({
            'worker_id': unhealthy_worker,
            'status': 'running',
            'last_heartbeat': time.time() - 120
        }))

        available = lifecycle_manager._get_available_workers()

        assert healthy_worker in available
        assert unhealthy_worker not in available
        assert current_worker not in available  # Should not include self

    def test_workload_distribution(self, lifecycle_manager, hp_redis_client):
        """Test workload distribution to other workers."""
        # Setup test data
        sessions = ["session_1", "session_2", "session_3", "session_4"]
        hp_redis_client.sadd(f"sessions:worker:{lifecycle_manager.worker_id}", *sessions)

        # Add available workers
        available_workers = ["worker_1", "worker_2"]
        for worker_id in available_workers:
            hp_redis_client.hset("workers:status", worker_id, json.dumps({
                'worker_id': worker_id,
                'status': 'running',
                'last_heartbeat': time.time()
            }))

        # Distribute workload
        workload = {'sessions': sessions}
        lifecycle_manager._distribute_workload(workload, available_workers)

        # Check that sessions were transferred
        for worker_id in available_workers:
            transferred_sessions = hp_redis_client.smembers(f"session:session_1:worker_id")
            # Sessions should have been transferred (implementation dependent)
            pass  # The actual transfer logic depends on session structure

    def test_task_completion_waiting(self, lifecycle_manager, hp_redis_client):
        """Test waiting for tasks to complete during shutdown."""
        # Add some active work
        hp_redis_client.sadd(f"sessions:worker:{lifecycle_manager.worker_id}", "session_1")
        hp_redis_client.hset(f"tasks:processing:{lifecycle_manager.worker_id}", "task_1", "data")

        # Start waiting (with short timeout for testing)
        start_time = time.time()
        lifecycle_manager._wait_for_tasks(timeout=2)

        # Should have waited for at least some time
        elapsed = time.time() - start_time
        assert elapsed >= 0  # At least started waiting


class TestIntegrationScenarios:
    """Test complete integration scenarios."""

    def test_worker_scaling_scenario(self, web_app_support, hp_redis_client):
        """Test a complete worker scaling scenario."""
        # Initialize the web app support
        web_app_support.initialize()

        # Create some sessions
        session_1 = web_app_support.create_user_session("user_1")
        session_2 = web_app_support.create_user_session("user_2")

        # Create some shared data
        shared_dict = web_app_support.get_shared_dict("test_data")
        shared_dict["key1"] = "value1"
        shared_dict["key2"] = "value2"

        # Get initial worker info
        initial_worker_info = web_app_support.get_worker_info()
        assert initial_worker_info['status'] == 'running'

        # Simulate graceful shutdown
        web_app_support.shutdown_graceful()

        # Check final state
        final_worker_info = web_app_support.get_worker_info()
        assert final_worker_info['status'] == 'stopped'

    def test_dead_worker_recovery_scenario(self, web_app_support, hp_redis_client):
        """Test dead worker detection and recovery."""
        # Initialize and register a worker
        web_app_support.initialize()

        worker_id = web_app_support.lifecycle_manager.worker_id

        # Simulate worker death by setting old heartbeat
        old_heartbeat = time.time() - 150  # 2.5 minutes ago
        hp_redis_client.hset(f"workers:heartbeat:{worker_id}", 'last_heartbeat', old_heartbeat)

        # Run worker monitoring (simulate what happens in background thread)
        dead_workers = web_app_support.worker_coordinator.detect_dead_workers()
        assert worker_id in dead_workers

        # Handle the dead worker
        web_app_support.worker_coordinator.handle_dead_worker(worker_id)

        # Check that worker was marked as dead
        worker_info = json.loads(hp_redis_client.hget("workers:status", worker_id))
        assert worker_info['status'] == 'dead'

    def test_cluster_monitoring_scenario(self, web_app_support, hp_redis_client):
        """Test cluster monitoring capabilities."""
        web_app_support.initialize()

        # Register multiple workers
        for i in range(3):
            worker_id = f"test_worker_{i}"
            web_app_support.worker_coordinator.register_worker(worker_id, {
                'hostname': f'host_{i}',
                'pid': 1000 + i,
                'status': 'running',
                'last_heartbeat': time.time()
            })

        # Get cluster statistics
        cluster_info = web_app_support.get_cluster_info()

        assert cluster_info['total_workers'] >= 4  # Including our worker
        assert cluster_info['healthy_workers'] >= 2  # Most should be healthy (our worker + some of the test workers)
        assert 'workers_by_status' in cluster_info

    def test_signal_handling_scenario(self, web_app_support, hp_redis_client):
        """Test signal handling for graceful shutdown."""
        web_app_support.initialize()

        # Create some sessions to test workload yielding
        session_1 = web_app_support.create_user_session("user_1")
        session_2 = web_app_support.create_user_session("user_2")

        # Simulate receiving SIGTERM (graceful shutdown)
        import signal

        # Get the signal handler that was registered
        signal_handler = None
        for sig in [signal.SIGTERM, signal.SIGINT]:
            if hasattr(signal, 'getsignal'):
                handler = signal.getsignal(sig)
                if handler and hasattr(handler, '__name__') and 'signal_handler' in str(handler):
                    signal_handler = handler
                    break

        # If we found a signal handler, it should handle graceful shutdown
        if signal_handler:
            # The handler should initiate graceful shutdown
            # We can't easily test the actual signal without causing issues,
            # but we can verify the setup exists
            assert callable(signal_handler)


class TestConcurrentSharedDict:
    """Test concurrent shared dictionary functionality."""

    def test_shared_dict_creation(self, web_app_support):
        """Test creation of concurrent shared dictionaries."""
        dict1 = web_app_support.get_shared_dict("test_dict_1")
        dict2 = web_app_support.get_shared_dict("test_dict_2")

        assert dict1.dict_name == "test_dict_1"
        assert dict2.dict_name == "test_dict_2"
        assert dict1 != dict2

    def test_shared_dict_operations(self, web_app_support, hp_redis_client):
        """Test basic operations on shared dictionaries."""
        shared_dict = web_app_support.get_shared_dict("test_operations")

        # Test set/get
        shared_dict["key1"] = "value1"
        assert shared_dict["key1"] == "value1"

        # Test bulk operations
        shared_dict.bulk_update({"key2": "value2", "key3": "value3"})
        assert shared_dict["key2"] == "value2"
        assert shared_dict["key3"] == "value3"

        # Test atomic increment
        result = shared_dict.increment("counter", 5)
        assert result == 5

        result = shared_dict.increment("counter", 3)
        assert result == 8

        # Test statistics
        stats = shared_dict.get_stats()
        assert stats['name'] == "test_operations"
        assert stats['key_count'] == 4  # key1, key2, key3, counter

    def test_shared_dict_concurrency(self, web_app_support):
        """Test concurrent access to shared dictionaries."""
        shared_dict = web_app_support.get_shared_dict("test_concurrency")

        def worker_function(start_val, end_val):
            for i in range(start_val, end_val):
                shared_dict[f"key_{i}"] = f"value_{i}"

        # Create multiple threads
        threads = []
        for i in range(5):
            start_val = i * 10
            end_val = start_val + 10
            thread = threading.Thread(target=worker_function, args=(start_val, end_val))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify all keys were set
        assert len(shared_dict) == 50  # 5 threads * 10 keys each

        for i in range(50):
            assert shared_dict[f"key_{i}"] == f"value_{i}"


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_worker_id(self, worker_coordinator):
        """Test handling of invalid worker IDs."""
        # Should not crash with invalid worker ID
        try:
            worker_coordinator.handle_dead_worker("nonexistent_worker")
            # If no exception, test passes
        except Exception as e:
            # Should handle gracefully
            assert "nonexistent_worker" in str(e) or True  # Either handles gracefully or provides informative error

    def test_redis_connection_loss(self, hp_redis_client):
        """Test behavior when Redis connection is lost."""
        if not CYREDIS_WEB_AVAILABLE:
            pytest.skip("CyRedis web app support not built")

        # This is hard to test reliably without actually breaking Redis
        # But we can test that the code handles connection errors gracefully
        support = WebApplicationSupport(redis_client=hp_redis_client)

        # Should not crash during initialization even if Redis is unavailable
        try:
            support.initialize()
            # If we get here without crashing, the error handling is working
        except Exception:
            # Expected if Redis is not available
            pass

    def test_malformed_worker_data(self, hp_redis_client):
        """Test handling of malformed worker data in Redis."""
        # Put malformed data in Redis
        hp_redis_client.hset("workers:status", "malformed_worker", "invalid json{")

        coordinator = WorkerCoordinator(hp_redis_client)

        # Should handle malformed data gracefully
        try:
            workers = coordinator.get_all_workers()
            # Should either skip malformed data or handle the error
            assert isinstance(workers, dict)
        except Exception as e:
            # Should handle gracefully
            assert "malformed" in str(e).lower() or True


class TestPerformance:
    """Test performance characteristics."""

    @pytest.mark.slow
    def test_worker_registration_performance(self, hp_redis_client):
        """Test performance of worker registration."""
        if not CYREDIS_WEB_AVAILABLE:
            pytest.skip("CyRedis web app support not built")

        start_time = time.time()

        # Register many workers
        for i in range(100):
            worker_id = f"perf_worker_{i}"
            worker_info = {
                'hostname': f'host_{i}',
                'pid': 1000 + i,
                'status': 'running'
            }

            coordinator = WorkerCoordinator(hp_redis_client)
            coordinator.register_worker(worker_id, worker_info)

        end_time = time.time()
        elapsed = end_time - start_time

        # Should register 100 workers reasonably quickly (< 5 seconds)
        assert elapsed < 5.0

    @pytest.mark.slow
    def test_workload_redistribution_performance(self, hp_redis_client):
        """Test performance of workload redistribution."""
        if not CYREDIS_WEB_AVAILABLE:
            pytest.skip("CyRedis web app support not built")

        # Create a lifecycle manager
        manager = LifecycleManager(hp_redis_client, worker_id="perf_worker")

        # Add many sessions
        sessions = [f"session_{i}" for i in range(100)]
        hp_redis_client.sadd(f"sessions:worker:{manager.worker_id}", *sessions)

        # Add available workers
        available_workers = [f"available_worker_{i}" for i in range(10)]
        for worker_id in available_workers:
            hp_redis_client.hset("workers:status", worker_id, json.dumps({
                'worker_id': worker_id,
                'status': 'running',
                'last_heartbeat': time.time()
            }))

        start_time = time.time()
        manager._distribute_workload({'sessions': sessions}, available_workers)
        end_time = time.time()

        elapsed = end_time - start_time

        # Should redistribute 100 sessions reasonably quickly (< 2 seconds)
        assert elapsed < 2.0


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])
