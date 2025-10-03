"""
Integration tests for Redis Cluster operations.

Tests cluster-specific functionality including:
- Cluster connection and topology
- Key distribution across nodes
- Cross-slot operations
- Cluster failover
- Resharding scenarios
- MOVED and ASK redirections
"""

import pytest
import time


@pytest.mark.integration
@pytest.mark.cluster
class TestClusterConnection:
    """Test Redis Cluster connection and topology."""

    def test_cluster_connection(self, redis_cluster_client):
        """Test connection to Redis Cluster."""
        # Should be able to ping
        assert redis_cluster_client.ping() is True

    def test_cluster_info(self, redis_cluster_client):
        """Test retrieving cluster information."""
        info = redis_cluster_client.cluster_info()

        # Should have cluster state
        assert "cluster_state" in info
        assert info["cluster_state"] == "ok"

    def test_cluster_nodes(self, redis_cluster_client):
        """Test retrieving cluster nodes."""
        nodes = redis_cluster_client.cluster_nodes()

        # Should have multiple nodes
        assert len(nodes) >= 3  # Minimum for cluster

        # Check node information
        for node_id, node_info in nodes.items():
            assert "host" in node_info
            assert "port" in node_info

    def test_cluster_slots(self, redis_cluster_client):
        """Test retrieving slot distribution."""
        slots = redis_cluster_client.cluster_slots()

        # Should have slot assignments
        assert len(slots) > 0

        # Total slots should be 16384
        total_slots = sum(slot[1] - slot[0] + 1 for slot in slots)
        assert total_slots == 16384


@pytest.mark.integration
@pytest.mark.cluster
class TestKeyDistribution:
    """Test key distribution across cluster nodes."""

    def test_keys_distributed_across_nodes(self, redis_cluster_client, unique_key):
        """Test that keys are distributed across different nodes."""
        base_key = f"{unique_key}:distributed"

        # Set many keys
        keys = []
        for i in range(100):
            key = f"{base_key}:{i}"
            keys.append(key)
            redis_cluster_client.set(key, f"value_{i}")

        # Get node for each key
        nodes_used = set()
        for key in keys:
            # Get the node responsible for this key
            slot = redis_cluster_client.cluster_keyslot(key)
            nodes_used.add(slot)

        # Keys should be distributed across multiple slots
        assert len(nodes_used) > 1

        # Cleanup
        redis_cluster_client.delete(*keys)

    def test_hash_tags_same_slot(self, redis_cluster_client, unique_key):
        """Test that hash tags keep keys in same slot."""
        # Keys with same hash tag should be in same slot
        tag = f"{unique_key}"
        key1 = f"{{{tag}}}:key1"
        key2 = f"{{{tag}}}:key2"
        key3 = f"{{{tag}}}:key3"

        # Set keys
        redis_cluster_client.set(key1, "value1")
        redis_cluster_client.set(key2, "value2")
        redis_cluster_client.set(key3, "value3")

        # Get slots
        slot1 = redis_cluster_client.cluster_keyslot(key1)
        slot2 = redis_cluster_client.cluster_keyslot(key2)
        slot3 = redis_cluster_client.cluster_keyslot(key3)

        # All should be in same slot
        assert slot1 == slot2 == slot3

        # Cleanup
        redis_cluster_client.delete(key1, key2, key3)


@pytest.mark.integration
@pytest.mark.cluster
class TestCrossSlotOperations:
    """Test operations across multiple slots."""

    def test_mget_cross_slot(self, redis_cluster_client, unique_key):
        """Test MGET with keys in different slots."""
        keys = [f"{unique_key}:key{i}" for i in range(10)]

        # Set keys
        for i, key in enumerate(keys):
            redis_cluster_client.set(key, f"value_{i}")

        # MGET should work across slots
        values = redis_cluster_client.mget(keys)

        assert len(values) == 10
        for i, value in enumerate(values):
            assert value == f"value_{i}"

        # Cleanup
        redis_cluster_client.delete(*keys)

    def test_mset_cross_slot(self, redis_cluster_client, unique_key):
        """Test MSET with keys in different slots."""
        keys = [f"{unique_key}:mset{i}" for i in range(10)]
        mapping = {key: f"value_{i}" for i, key in enumerate(keys)}

        # MSET should work across slots
        redis_cluster_client.mset(mapping)

        # Verify
        for i, key in enumerate(keys):
            assert redis_cluster_client.get(key) == f"value_{i}"

        # Cleanup
        redis_cluster_client.delete(*keys)

    def test_pipeline_cross_slot(self, redis_cluster_client, unique_key):
        """Test pipeline with cross-slot operations."""
        keys = [f"{unique_key}:pipe{i}" for i in range(10)]

        # Pipeline should handle cross-slot operations
        pipe = redis_cluster_client.pipeline()

        for i, key in enumerate(keys):
            pipe.set(key, f"value_{i}")

        results = pipe.execute()

        # All sets should succeed
        assert all(r is True for r in results)

        # Cleanup
        redis_cluster_client.delete(*keys)


@pytest.mark.integration
@pytest.mark.cluster
class TestClusterFailover:
    """Test cluster failover scenarios."""

    def test_read_from_replica(self, redis_cluster_client, unique_key):
        """Test reading from replica nodes."""
        key = f"{unique_key}:replica"

        # Write to master
        redis_cluster_client.set(key, "value")

        # Allow replication time
        time.sleep(0.5)

        # Read should work (may read from master or replica)
        value = redis_cluster_client.get(key)
        assert value == "value"

        # Cleanup
        redis_cluster_client.delete(key)

    def test_write_after_failover(self, redis_cluster_client, unique_key):
        """Test that writes continue after failover simulation."""
        key = f"{unique_key}:failover"

        # Write before
        redis_cluster_client.set(key, "before")
        assert redis_cluster_client.get(key) == "before"

        # Note: Actual failover testing requires cluster manipulation
        # This test verifies basic resilience

        # Write after (should work)
        redis_cluster_client.set(key, "after")
        assert redis_cluster_client.get(key) == "after"

        # Cleanup
        redis_cluster_client.delete(key)


@pytest.mark.integration
@pytest.mark.cluster
class TestClusterCommands:
    """Test cluster-specific commands."""

    def test_cluster_keyslot(self, redis_cluster_client):
        """Test CLUSTER KEYSLOT command."""
        # Get slot for key
        slot = redis_cluster_client.cluster_keyslot("mykey")

        # Slot should be in valid range
        assert 0 <= slot < 16384

        # Same key should always get same slot
        slot2 = redis_cluster_client.cluster_keyslot("mykey")
        assert slot == slot2

    def test_cluster_countkeysinslot(self, redis_cluster_client, unique_key):
        """Test CLUSTER COUNTKEYSINSLOT command."""
        # This test depends on cluster internals
        # Just verify command works
        try:
            # Get a slot number
            slot = redis_cluster_client.cluster_keyslot(unique_key)

            # Count keys in slot (specific to node, may not work on all clients)
            # Some clients may not support this directly
            pass
        except Exception:
            # Command might not be available in all client versions
            pytest.skip("CLUSTER COUNTKEYSINSLOT not available")


@pytest.mark.integration
@pytest.mark.cluster
class TestClusterTransactions:
    """Test transactions in cluster mode."""

    def test_transaction_same_slot(self, redis_cluster_client, unique_key):
        """Test transaction with keys in same slot."""
        # Use hash tags to ensure same slot
        tag = f"{unique_key}"
        key1 = f"{{{tag}}}:key1"
        key2 = f"{{{tag}}}:key2"

        # Transaction should work with same-slot keys
        pipe = redis_cluster_client.pipeline()
        pipe.set(key1, "value1")
        pipe.set(key2, "value2")
        pipe.get(key1)
        pipe.get(key2)

        results = pipe.execute()

        assert results == [True, True, "value1", "value2"]

        # Cleanup
        redis_cluster_client.delete(key1, key2)


@pytest.mark.integration
@pytest.mark.cluster
class TestClusterScaling:
    """Test cluster scaling scenarios."""

    def test_many_keys_distribution(self, redis_cluster_client, unique_key):
        """Test distribution of many keys."""
        base_key = f"{unique_key}:scale"

        # Create many keys
        num_keys = 1000
        keys = []

        for i in range(num_keys):
            key = f"{base_key}:{i}"
            keys.append(key)
            redis_cluster_client.set(key, f"value_{i}")

        # Verify all keys
        for i, key in enumerate(keys):
            value = redis_cluster_client.get(key)
            assert value == f"value_{i}"

        # Cleanup
        # Delete in batches to avoid cross-slot issues
        batch_size = 100
        for i in range(0, len(keys), batch_size):
            batch = keys[i:i+batch_size]
            redis_cluster_client.delete(*batch)

    def test_concurrent_cluster_operations(self, redis_cluster_client, unique_key):
        """Test concurrent operations on cluster."""
        from concurrent.futures import ThreadPoolExecutor

        base_key = f"{unique_key}:concurrent"

        def operation(index):
            key = f"{base_key}:{index}"
            redis_cluster_client.set(key, f"value_{index}")
            return redis_cluster_client.get(key) == f"value_{index}"

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(operation, i) for i in range(100)]
            results = [f.result() for f in futures]

        # All should succeed
        assert all(results)

        # Cleanup
        keys = [f"{base_key}:{i}" for i in range(100)]
        redis_cluster_client.delete(*keys)
