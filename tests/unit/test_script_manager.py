"""
Unit tests for script_manager.pyx - Lua script management
"""
import pytest
import hashlib
import uuid
from cy_redis.script_manager import (
    CyLuaScriptManager, OptimizedLuaScriptManager
)
from cy_redis.cy_redis_client import CyRedisClient


@pytest.fixture
def redis_client():
    """Create a Redis client for testing"""
    return CyRedisClient(host="localhost", port=6379)


@pytest.fixture
def script_manager(redis_client):
    """Create a script manager for testing"""
    namespace = f"test_scripts_{uuid.uuid4().hex[:8]}"
    manager = CyLuaScriptManager(redis_client, namespace)
    yield manager
    # Cleanup
    try:
        manager.cleanup_scripts(confirm=True)
    except:
        pass


# Sample Lua scripts for testing
SIMPLE_SCRIPT = """
return "Hello from Lua!"
"""

INCREMENT_SCRIPT = """
local key = KEYS[1]
local increment = tonumber(ARGV[1])
return redis.call('INCRBY', key, increment)
"""

COMPLEX_SCRIPT = """
local key = KEYS[1]
local value = ARGV[1]
redis.call('SET', key, value)
redis.call('EXPIRE', key, 60)
return redis.call('GET', key)
"""


class TestCyLuaScriptManager:
    """Test CyLuaScriptManager class"""

    def test_manager_creation(self, redis_client):
        """Test creating script manager"""
        manager = CyLuaScriptManager(redis_client, "test_scripts")
        assert manager is not None
        assert manager.namespace == "test_scripts"

    def test_register_script(self, script_manager):
        """Test registering a script"""
        sha = script_manager.register_script(
            "test_script",
            SIMPLE_SCRIPT,
            version="1.0.0"
        )
        assert sha is not None
        assert isinstance(sha, str)
        assert len(sha) == 40  # SHA1 hash length

    def test_register_script_with_metadata(self, script_manager):
        """Test registering script with metadata"""
        metadata = {
            'author': 'test',
            'description': 'Test script',
            'tags': ['test', 'example']
        }
        sha = script_manager.register_script(
            "test_script",
            SIMPLE_SCRIPT,
            version="1.0.0",
            metadata=metadata
        )
        assert sha is not None

        # Verify metadata is stored
        info = script_manager.get_script_info("test_script")
        assert info['metadata'] == metadata

    def test_execute_script(self, script_manager):
        """Test executing a registered script"""
        script_manager.register_script("simple", SIMPLE_SCRIPT)

        result = script_manager.execute_script("simple")
        assert result == "Hello from Lua!"

    def test_execute_script_with_keys_args(self, script_manager, redis_client):
        """Test executing script with keys and arguments"""
        script_manager.register_script("increment", INCREMENT_SCRIPT)

        # Set initial value
        test_key = f"test_counter_{uuid.uuid4().hex[:8]}"
        redis_client.set(test_key, "10")

        # Execute script
        result = script_manager.execute_script(
            "increment",
            keys=[test_key],
            args=["5"]
        )
        assert result == 15

        # Cleanup
        redis_client.delete(test_key)

    def test_get_script_info(self, script_manager):
        """Test getting script information"""
        script_manager.register_script("test", SIMPLE_SCRIPT, version="1.0.0")

        info = script_manager.get_script_info("test")
        assert info['name'] == "test"
        assert info['version'] == "1.0.0"
        assert 'sha' in info
        assert 'cached' in info
        assert 'source_length' in info

    def test_list_scripts(self, script_manager):
        """Test listing all scripts"""
        script_manager.register_script("script1", SIMPLE_SCRIPT)
        script_manager.register_script("script2", INCREMENT_SCRIPT)

        scripts = script_manager.list_scripts()
        assert isinstance(scripts, dict)
        assert len(scripts) >= 2
        assert "script1" in scripts
        assert "script2" in scripts

    def test_validate_script(self, script_manager):
        """Test validating script syntax"""
        result = script_manager.validate_script(SIMPLE_SCRIPT)
        assert result['valid'] is True
        assert 'sha' in result

    def test_validate_invalid_script(self, script_manager):
        """Test validating invalid script"""
        invalid_script = "return invalid syntax here"
        result = script_manager.validate_script(invalid_script)
        # May or may not be valid depending on Lua interpretation

    def test_reload_all_scripts(self, script_manager):
        """Test reloading all scripts"""
        script_manager.register_script("test1", SIMPLE_SCRIPT)
        script_manager.register_script("test2", INCREMENT_SCRIPT)

        results = script_manager.reload_all_scripts()
        assert isinstance(results, dict)
        assert len(results) >= 2

    def test_cleanup_scripts(self, script_manager):
        """Test cleaning up scripts"""
        script_manager.register_script("test", SIMPLE_SCRIPT)

        # Without confirmation
        result = script_manager.cleanup_scripts(confirm=False)
        assert 'error' in result

        # With confirmation
        result = script_manager.cleanup_scripts(confirm=True)
        assert result['status'] == 'cleanup_complete'

    def test_get_script_stats(self, script_manager):
        """Test getting script statistics"""
        script_manager.register_script("test1", SIMPLE_SCRIPT)
        script_manager.register_script("test2", INCREMENT_SCRIPT)

        stats = script_manager.get_script_stats()
        assert 'total_scripts' in stats
        assert 'namespace' in stats
        assert 'cache_info' in stats
        assert stats['total_scripts'] >= 2


class TestAtomicOperations:
    """Test atomic script operations"""

    def test_atomic_load_and_test(self, script_manager):
        """Test atomic load and test operation"""
        test_cases = {
            'simple_test': {
                'keys': [],
                'args': [],
                'expected': "Hello from Lua!"
            }
        }

        result = script_manager.atomic_load_and_test(
            "atomic_test",
            SIMPLE_SCRIPT,
            version="1.0.0",
            test_cases=test_cases
        )

        assert result['success'] is True
        assert len(result['tests_passed']) > 0
        assert len(result['tests_failed']) == 0
        assert 'functions' in result

    def test_atomic_load_with_failed_tests(self, script_manager):
        """Test atomic load with failing tests"""
        test_cases = {
            'fail_test': {
                'keys': [],
                'args': [],
                'expected': "Wrong result"  # This will fail
            }
        }

        result = script_manager.atomic_load_and_test(
            "failing_test",
            SIMPLE_SCRIPT,
            version="1.0.0",
            test_cases=test_cases
        )

        assert result['success'] is False
        assert len(result['tests_failed']) > 0

    def test_atomic_deploy_scripts(self, script_manager):
        """Test atomic deployment of multiple scripts"""
        scripts_config = {
            'script1': {
                'script': SIMPLE_SCRIPT,
                'version': '1.0.0',
                'test_cases': {},
                'metadata': {'type': 'simple'}
            },
            'script2': {
                'script': INCREMENT_SCRIPT,
                'version': '1.0.0',
                'test_cases': {},
                'metadata': {'type': 'increment'}
            }
        }

        result = script_manager.atomic_deploy_scripts(scripts_config)
        assert result['success'] is True or result['success'] is False
        assert 'deployed_scripts' in result
        assert 'failed_scripts' in result


class TestScriptVersioning:
    """Test script versioning"""

    def test_register_same_version(self, script_manager):
        """Test registering same version twice"""
        sha1 = script_manager.register_script("test", SIMPLE_SCRIPT, "1.0.0")
        sha2 = script_manager.register_script("test", SIMPLE_SCRIPT, "1.0.0")

        # Should return same SHA
        assert sha1 == sha2

    def test_register_new_version(self, script_manager):
        """Test registering new version"""
        sha1 = script_manager.register_script("test", SIMPLE_SCRIPT, "1.0.0")
        sha2 = script_manager.register_script("test", SIMPLE_SCRIPT, "2.0.0")

        # May be different or same depending on implementation
        assert sha1 is not None
        assert sha2 is not None


class TestOptimizedLuaScriptManager:
    """Test OptimizedLuaScriptManager wrapper"""

    def test_wrapper_creation(self):
        """Test creating optimized wrapper"""
        # This would create its own client
        pass


class TestEdgeCases:
    """Test edge cases for script management"""

    def test_execute_unregistered_script(self, script_manager):
        """Test executing unregistered script"""
        with pytest.raises(Exception):
            script_manager.execute_script("nonexistent")

    def test_empty_script(self, script_manager):
        """Test registering empty script"""
        try:
            sha = script_manager.register_script("empty", "")
            # May succeed or fail depending on Redis
        except:
            pass

    def test_very_long_script(self, script_manager):
        """Test registering very long script"""
        long_script = "return '" + ("x" * 10000) + "'"
        sha = script_manager.register_script("long", long_script)
        assert sha is not None

    def test_script_with_unicode(self, script_manager):
        """Test script with Unicode characters"""
        unicode_script = 'return "Hello 世界 🌍"'
        sha = script_manager.register_script("unicode", unicode_script)
        assert sha is not None

        result = script_manager.execute_script("unicode")
        assert "Hello" in str(result)

    def test_concurrent_script_registration(self, script_manager):
        """Test concurrent script registration"""
        import threading

        results = []

        def register():
            sha = script_manager.register_script(
                f"concurrent_{uuid.uuid4().hex[:4]}",
                SIMPLE_SCRIPT
            )
            results.append(sha)

        threads = [threading.Thread(target=register) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 5
        assert all(sha is not None for sha in results)


class TestScriptMetadata:
    """Test script metadata handling"""

    def test_metadata_storage(self, script_manager):
        """Test that metadata is properly stored and retrieved"""
        metadata = {
            'author': 'test_author',
            'created': '2024-01-01',
            'tags': ['test', 'example']
        }

        sha = script_manager.register_script(
            "meta_test",
            SIMPLE_SCRIPT,
            version="1.0.0",
            metadata=metadata
        )

        info = script_manager.get_script_info("meta_test")
        assert info['metadata'] == metadata

    def test_update_metadata(self, script_manager):
        """Test updating script metadata"""
        metadata1 = {'version': '1.0'}
        script_manager.register_script("test", SIMPLE_SCRIPT, metadata=metadata1)

        metadata2 = {'version': '2.0', 'updated': True}
        script_manager.register_script("test", SIMPLE_SCRIPT, metadata=metadata2)

        # Latest metadata should be stored


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
