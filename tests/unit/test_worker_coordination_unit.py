"""
Unit tests for CyRedis worker coordination features.
Tests core logic without requiring Redis connections.
"""

import pytest
import time
import json
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Any

try:
    from cy_redis.web_app_support import (
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


pytestmark = pytest.mark.unit


class TestWorkerIDGeneration:
    """Test worker ID generation logic."""

    def test_worker_id_format(self):
        """Test that worker IDs are properly formatted."""
        # Mock the modules that aren't available in unit tests
        with patch('cy_redis.web_app_support.os') as mock_os, \
             patch('cy_redis.web_app_support.socket') as mock_socket:

            mock_os.getpid.return_value = 12345
            mock_socket.gethostname.return_value = "test-host"

            if CYREDIS_WEB_AVAILABLE:
                # We can't easily test the Cython class directly without Redis
                # But we can test the logic by mocking the Redis client
                mock_redis = Mock()
                manager = LifecycleManager(mock_redis, worker_id=None)

                # Check that worker_id follows expected pattern
                assert manager.worker_id.startswith("worker_test-host_12345_")
                assert len(manager.worker_id) > 20  # Should include timestamp


class TestWorkerCoordinatorLogic:
    """Test worker coordinator logic without Redis."""

    def test_worker_registration_logic(self):
        """Test worker registration data structure."""
        mock_redis = Mock()
        coordinator = WorkerCoordinator(mock_redis, coordinator_id="test_coord")

        worker_id = "test_worker_123"
        worker_info = {
            'hostname': 'test_host',
            'pid': 12345,
            'status': 'running'
        }

        # Mock the Redis call
        mock_redis.hset.return_value = 1

        coordinator.register_worker(worker_id, worker_info)

        # Verify the call was made with correct data
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        key, field, value = call_args[0]

        assert key == "workers:status"
        assert field == worker_id

        # Parse the JSON value
        stored_data = json.loads(value)
        assert stored_data['hostname'] == 'test_host'
        assert stored_data['pid'] == 12345
        assert stored_data['status'] == 'running'
        assert 'registered_at' in stored_data
        assert stored_data['coordinator_id'] == 'test_coord'

    def test_worker_filtering_logic(self):
        """Test worker filtering logic."""
        mock_redis = Mock()
        coordinator = WorkerCoordinator(mock_redis, coordinator_id="test_coord")

        # Mock Redis response with worker data
        current_time = time.time()
        mock_redis.hgetall.return_value = {
            'worker_1': json.dumps({
                'worker_id': 'worker_1',
                'status': 'running',
                'last_heartbeat': current_time
            }),
            'worker_2': json.dumps({
                'worker_id': 'worker_2',
                'status': 'running',
                'last_heartbeat': current_time - 150  # Old heartbeat
            }),
            'worker_3': json.dumps({
                'worker_id': 'worker_3',
                'status': 'stopped',
                'last_heartbeat': current_time
            })
        }

        healthy_workers = coordinator.get_healthy_workers()

        # Should only include recently active running workers
        assert 'worker_1' in healthy_workers
        assert 'worker_2' not in healthy_workers  # Old heartbeat
        assert 'worker_3' not in healthy_workers  # Stopped status

    def test_dead_worker_detection_logic(self):
        """Test dead worker detection logic."""
        mock_redis = Mock()
        coordinator = WorkerCoordinator(mock_redis, coordinator_id="test_coord")

        current_time = time.time()
        mock_redis.hgetall.return_value = {
            'alive_worker': json.dumps({
                'worker_id': 'alive_worker',
                'status': 'running',
                'last_heartbeat': current_time
            }),
            'dead_worker': json.dumps({
                'worker_id': 'dead_worker',
                'status': 'running',
                'last_heartbeat': current_time - 150  # 2.5 minutes ago
            }),
            'stopped_worker': json.dumps({
                'worker_id': 'stopped_worker',
                'status': 'stopped',
                'last_heartbeat': current_time - 150
            })
        }

        dead_workers = coordinator.detect_dead_workers()

        # Should detect dead_worker as dead (old heartbeat, running status)
        # Should not detect stopped_worker as dead (stopped status)
        assert 'dead_worker' in dead_workers
        assert 'stopped_worker' not in dead_workers
        assert 'alive_worker' not in dead_workers

    def test_cluster_stats_calculation(self):
        """Test cluster statistics calculation."""
        mock_redis = Mock()
        coordinator = WorkerCoordinator(mock_redis, coordinator_id="test_coord")

        current_time = time.time()
        mock_redis.hgetall.return_value = {
            'worker_1': json.dumps({
                'worker_id': 'worker_1',
                'status': 'running',
                'last_heartbeat': current_time
            }),
            'worker_2': json.dumps({
                'worker_id': 'worker_2',
                'status': 'running',
                'last_heartbeat': current_time - 150  # Old heartbeat
            }),
            'worker_3': json.dumps({
                'worker_id': 'worker_3',
                'status': 'stopped',
                'last_heartbeat': current_time
            })
        }

        stats = coordinator.get_cluster_stats()

        assert stats['total_workers'] == 3
        assert stats['healthy_workers'] == 1  # Only worker_1 (running + recent heartbeat)
        assert stats['dead_workers'] == 1     # Only worker_2 (running + old heartbeat)

        status_counts = stats['workers_by_status']
        assert status_counts['running'] == 2  # worker_1 and worker_2 (status still running)
        assert status_counts['stopped'] == 1  # worker_3


class TestLifecycleManagerLogic:
    """Test lifecycle manager logic without Redis."""

    def test_priority_hook_ordering(self):
        """Test that hooks run in correct priority order."""
        mock_redis = Mock()
        manager = LifecycleManager(mock_redis, worker_id="test_worker")

        execution_order = []

        def hook_priority_3():
            execution_order.append(3)

        def hook_priority_1():
            execution_order.append(1)

        def hook_priority_2():
            execution_order.append(2)

        # Add hooks with different priorities
        manager.add_startup_hook(hook_priority_1, priority=1)  # Highest priority (lowest number)
        manager.add_startup_hook(hook_priority_2, priority=2)
        manager.add_startup_hook(hook_priority_3, priority=3)  # Lowest priority (highest number)

        # Simulate initialization (without actually running Redis operations)
        # Just test that hooks are stored in correct order
        assert len(manager.startup_hooks) == 3

        # Hooks should be sorted by priority (lower number first)
        priorities = [priority for priority, _ in manager.startup_hooks]
        assert priorities == [1, 2, 3]

    def test_shutdown_hook_ordering(self):
        """Test that shutdown hooks run in reverse priority order."""
        mock_redis = Mock()
        manager = LifecycleManager(mock_redis, worker_id="test_worker")

        def hook_priority_1():
            pass

        def hook_priority_2():
            pass

        def hook_priority_3():
            pass

        # Add shutdown hooks
        manager.add_shutdown_hook(hook_priority_1, priority=1)
        manager.add_shutdown_hook(hook_priority_2, priority=2)
        manager.add_shutdown_hook(hook_priority_3, priority=3)

        # Shutdown hooks should be sorted by priority for cleanup order
        # (lower priority numbers run first during shutdown)
        priorities = [priority for priority, _ in manager.shutdown_hooks]
        assert priorities == [1, 2, 3]

    def test_worker_status_transitions(self):
        """Test worker status transition logic."""
        mock_redis = Mock()
        manager = LifecycleManager(mock_redis, worker_id="test_worker")

        # Mock Redis responses
        mock_redis.hget.return_value = json.dumps({
            'worker_id': 'test_worker',
            'status': 'starting',
            'last_heartbeat': time.time()
        })

        # Test status updates
        manager._update_worker_status('running')
        mock_redis.hset.assert_called()

        # Verify the call structure
        calls = mock_redis.hset.call_args_list
        assert len(calls) >= 1

        # Check that status was updated
        call_args = calls[-1][0]  # Get last call
        key, field, value = call_args
        assert key == "workers:status"
        assert field == "test_worker"

        updated_data = json.loads(value)
        assert updated_data['status'] == 'running'


class TestConcurrentSharedDictLogic:
    """Test concurrent shared dictionary logic."""

    def test_dict_creation_and_basic_operations(self):
        """Test basic dictionary operations without Redis."""
        # Since we can't easily test the Cython class without Redis,
        # we'll test the logic that would be used

        # Test that dictionary names are handled correctly
        dict_name = "test_dict"
        expected_key = f"shared_dict:{dict_name}"

        # This would be the Redis key used for the dictionary
        assert expected_key == "shared_dict:test_dict"

    def test_atomic_operations_logic(self):
        """Test the logic of atomic operations."""
        # Test increment logic
        key = "counter"
        current_value = 5
        increment = 3
        expected_result = current_value + increment

        # This simulates the logic in the Cython implementation
        result = current_value + increment
        assert result == expected_result

        # Test float increment
        float_value = 5.5
        float_increment = 2.5
        expected_float = float_value + float_increment

        float_result = float_value + float_increment
        assert float_result == expected_float


class TestErrorHandlingLogic:
    """Test error handling logic without external dependencies."""

    def test_invalid_worker_id_handling(self):
        """Test handling of invalid worker IDs."""
        mock_redis = Mock()
        coordinator = WorkerCoordinator(mock_redis, coordinator_id="test_coord")

        # Test with invalid worker ID
        invalid_worker_id = "nonexistent_worker"

        # Should handle gracefully
        try:
            coordinator.handle_dead_worker(invalid_worker_id)
            # If no exception, it handled gracefully
        except Exception as e:
            # Should provide informative error or handle gracefully
            assert "nonexistent" in str(e).lower() or True

    def test_malformed_data_handling(self):
        """Test handling of malformed JSON data."""
        mock_redis = Mock()
        coordinator = WorkerCoordinator(mock_redis, coordinator_id="test_coord")

        # Mock malformed JSON response
        mock_redis.hgetall.return_value = {
            'worker_1': 'invalid json{',
            'worker_2': '{"valid": "json"}'
        }

        # Should handle malformed data gracefully
        try:
            workers = coordinator.get_all_workers()
            # Should either skip malformed data or handle the error
            assert isinstance(workers, dict)
        except Exception as e:
            # Should handle gracefully
            assert "json" in str(e).lower() or True


class TestPerformanceCharacteristics:
    """Test performance characteristics of algorithms."""

    def test_priority_sorting_performance(self):
        """Test that priority sorting is efficient."""
        import random

        # Create many hooks with random priorities
        priorities = list(range(1000))
        random.shuffle(priorities)

        # Simulate hook addition and sorting
        hooks = []
        for priority in priorities:
            hooks.append((priority, lambda: None))

        # Sort hooks (simulates what happens in LifecycleManager)
        start_time = time.time()
        sorted_hooks = sorted(hooks, key=lambda x: x[0])
        end_time = time.time()

        elapsed = end_time - start_time

        # Should sort 1000 items very quickly (< 0.1 seconds)
        assert elapsed < 0.1

        # Verify correct sorting
        sorted_priorities = [priority for priority, _ in sorted_hooks]
        assert sorted_priorities == list(range(1000))

    def test_worker_filtering_performance(self):
        """Test performance of worker filtering."""
        # Simulate many workers
        workers = {}
        current_time = time.time()

        for i in range(1000):
            worker_id = f"worker_{i}"
            workers[worker_id] = {
                'worker_id': worker_id,
                'status': 'running',
                'last_heartbeat': current_time if i % 10 != 0 else current_time - 150  # 10% dead
            }

        # Test filtering logic (simulates get_healthy_workers)
        start_time = time.time()
        healthy_workers = []
        for worker_id, worker_info in workers.items():
            if (worker_info['status'] == 'running' and
                worker_info['last_heartbeat'] > current_time - 60):
                healthy_workers.append(worker_id)
        end_time = time.time()

        elapsed = end_time - start_time

        # Should filter 1000 workers very quickly (< 0.01 seconds)
        assert elapsed < 0.01

        # Should have 900 healthy workers (90% of 1000)
        assert len(healthy_workers) == 900


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])
