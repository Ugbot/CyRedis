"""
Test the pytest configuration and fixtures.
This is a meta-test to validate that the test infrastructure is working correctly.
"""

import pytest
import sys
from pathlib import Path


@pytest.mark.unit
def test_python_path_setup(project_root):
    """Test that project root is in Python path."""
    assert str(project_root) in sys.path or any(
        str(project_root) in p for p in sys.path
    )


@pytest.mark.unit
def test_project_root_fixture(project_root):
    """Test that project_root fixture returns valid path."""
    assert isinstance(project_root, Path)
    assert project_root.exists()
    assert (project_root / "cy_redis").exists()


@pytest.mark.unit
def test_redis_configuration(redis_host, redis_port, redis_db):
    """Test Redis configuration fixtures."""
    assert isinstance(redis_host, str)
    assert isinstance(redis_port, int)
    assert isinstance(redis_db, int)
    assert redis_port > 0
    assert redis_db >= 0


@pytest.mark.unit
def test_random_key_generator(random_key):
    """Test random key generation fixture."""
    key1 = random_key("test")
    key2 = random_key("test")

    assert key1.startswith("test:")
    assert key2.startswith("test:")
    assert key1 != key2  # Should be unique


@pytest.mark.unit
def test_sample_data_fixture(sample_data):
    """Test sample data fixture."""
    assert isinstance(sample_data, dict)
    assert "string" in sample_data
    assert "number" in sample_data
    assert "list" in sample_data
    assert sample_data["number"] == 42


@pytest.mark.unit
def test_bulk_test_data_generator(bulk_test_data):
    """Test bulk data generation."""
    data = bulk_test_data(10)
    assert len(data) == 10
    assert all("id" in item for item in data)
    assert all("timestamp" in item for item in data)


@pytest.mark.unit
def test_performance_timer(perf_timer):
    """Test performance timer fixture."""
    import time

    with perf_timer("test_operation") as timer:
        time.sleep(0.01)

    assert timer.duration > 0.01
    assert timer.elapsed > 0.01


@pytest.mark.unit
def test_test_namespace(test_namespace):
    """Test namespace fixture."""
    assert isinstance(test_namespace, str)
    assert "namespace:" in test_namespace


@pytest.mark.unit
def test_test_config(test_config):
    """Test configuration fixture."""
    assert isinstance(test_config, dict)
    assert "timeout" in test_config
    assert "max_retries" in test_config
    assert test_config["timeout"] > 0


@pytest.mark.unit
def test_cleanup_temp_files(cleanup_temp_files, tmp_path):
    """Test temp file cleanup fixture."""
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("test")

    cleanup_temp_files(temp_file)
    # File should be cleaned up after test


@pytest.mark.integration
@pytest.mark.requires_redis
def test_redis_client_fixture(redis_client, random_key):
    """Test Redis client fixture (requires Redis)."""
    key = random_key("config_test")
    redis_client.set(key, "test_value")
    assert redis_client.get(key) == "test_value"


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.async_test
async def test_async_redis_client_fixture(async_redis_client, random_key):
    """Test async Redis client fixture (requires Redis)."""
    key = random_key("async_config_test")
    await async_redis_client.set(key, "test_value")
    value = await async_redis_client.get(key)
    assert value == "test_value"


@pytest.mark.unit
def test_markers_configured():
    """Test that custom markers are configured."""
    # This test verifies that pytest is configured correctly
    # If markers aren't configured, pytest will warn about unknown markers
    pass


if __name__ == "__main__":
    # Run this test file directly
    pytest.main([__file__, "-v", "-m", "unit"])
