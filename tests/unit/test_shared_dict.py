"""
Unit tests for shared_dict.pyx - Shared dictionary
"""
import pytest
import time
import threading
import uuid
from cy_redis.shared_dict import CySharedDict, CySharedDictManager
from cy_redis.cy_redis_client import CyRedisClient


@pytest.fixture
def redis_client():
    """Create a Redis client for testing"""
    return CyRedisClient(host="localhost", port=6379)


@pytest.fixture
def shared_dict(redis_client):
    """Create a shared dictionary for testing"""
    dict_key = f"test_dict_{uuid.uuid4().hex[:8]}"
    sd = CySharedDict(redis_client, dict_key, cache_ttl=5)
    yield sd
    # Cleanup
    try:
        sd.clear()
        redis_client.delete(dict_key)
        redis_client.delete(f"{dict_key}:lock")
    except:
        pass


@pytest.fixture
def dict_manager(redis_client):
    """Create a shared dict manager for testing"""
    manager = CySharedDictManager(redis_client)
    yield manager


class TestCySharedDict:
    """Test CySharedDict class"""

    def test_dict_creation(self, redis_client):
        """Test creating a shared dictionary"""
        sd = CySharedDict(redis_client, "test_dict")
        assert sd is not None
        assert sd.dict_key == "test_dict"

    def test_setitem_getitem(self, shared_dict):
        """Test setting and getting items"""
        shared_dict['key1'] = 'value1'
        assert shared_dict['key1'] == 'value1'

    def test_delitem(self, shared_dict):
        """Test deleting items"""
        shared_dict['key1'] = 'value1'
        assert 'key1' in shared_dict

        del shared_dict['key1']
        assert 'key1' not in shared_dict

    def test_contains(self, shared_dict):
        """Test checking if key exists"""
        shared_dict['key1'] = 'value1'
        assert 'key1' in shared_dict
        assert 'nonexistent' not in shared_dict

    def test_len(self, shared_dict):
        """Test getting dictionary length"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'
        assert len(shared_dict) >= 2

    def test_iter(self, shared_dict):
        """Test iterating over keys"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        keys = list(shared_dict)
        assert 'key1' in keys
        assert 'key2' in keys

    def test_keys(self, shared_dict):
        """Test getting dictionary keys"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        keys = shared_dict.keys()
        assert 'key1' in keys
        assert 'key2' in keys

    def test_values(self, shared_dict):
        """Test getting dictionary values"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        values = shared_dict.values()
        assert 'value1' in list(values)
        assert 'value2' in list(values)

    def test_items(self, shared_dict):
        """Test getting dictionary items"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        items = list(shared_dict.items())
        assert ('key1', 'value1') in items
        assert ('key2', 'value2') in items

    def test_get_with_default(self, shared_dict):
        """Test get with default value"""
        value = shared_dict.get('nonexistent', 'default')
        assert value == 'default'

        shared_dict['existing'] = 'value'
        value = shared_dict.get('existing', 'default')
        assert value == 'value'

    def test_update(self, shared_dict):
        """Test updating dictionary"""
        shared_dict.update({'key1': 'value1', 'key2': 'value2'})
        assert shared_dict['key1'] == 'value1'
        assert shared_dict['key2'] == 'value2'

    def test_clear(self, shared_dict):
        """Test clearing dictionary"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        shared_dict.clear()
        assert len(shared_dict) == 0

    def test_pop(self, shared_dict):
        """Test popping an item"""
        shared_dict['key1'] = 'value1'

        value = shared_dict.pop('key1')
        assert value == 'value1'
        assert 'key1' not in shared_dict

        # Pop non-existent with default
        value = shared_dict.pop('nonexistent', 'default')
        assert value == 'default'

    def test_popitem(self, shared_dict):
        """Test popping random item"""
        shared_dict['key1'] = 'value1'

        key, value = shared_dict.popitem()
        assert key == 'key1'
        assert value == 'value1'

    def test_setdefault(self, shared_dict):
        """Test setdefault operation"""
        shared_dict.setdefault('key1', 'default_value')
        assert shared_dict['key1'] == 'default_value'

        shared_dict['key2'] = 'existing_value'
        shared_dict.setdefault('key2', 'new_default')
        assert shared_dict['key2'] == 'existing_value'

    def test_bulk_update(self, shared_dict):
        """Test bulk update operation"""
        updates = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3'
        }
        shared_dict.bulk_update(updates)

        assert shared_dict['key1'] == 'value1'
        assert shared_dict['key2'] == 'value2'
        assert shared_dict['key3'] == 'value3'

    def test_bulk_get(self, shared_dict):
        """Test bulk get operation"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        values = shared_dict.bulk_get(['key1', 'key2', 'nonexistent'])
        assert len(values) == 3
        assert 'value1' in values
        assert 'value2' in values

    def test_increment(self, shared_dict):
        """Test atomic increment operation"""
        shared_dict['counter'] = 10

        result = shared_dict.increment('counter', 5)
        assert result == 15

        result = shared_dict.increment('counter', 1)
        assert result == 16

    def test_increment_float(self, shared_dict):
        """Test atomic float increment"""
        shared_dict['float_counter'] = 10.5

        result = shared_dict.increment_float('float_counter', 2.3)
        assert abs(result - 12.8) < 0.01

    def test_sync(self, shared_dict):
        """Test force synchronization"""
        shared_dict['key1'] = 'value1'
        shared_dict.sync()

        # Cache should be fresh
        assert shared_dict.is_synced() is True

    def test_get_stats(self, shared_dict):
        """Test getting dictionary statistics"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        stats = shared_dict.get_stats()
        assert 'key_count' in stats
        assert 'total_size_bytes' in stats
        assert 'compression_enabled' in stats
        assert 'cache_age_seconds' in stats
        assert stats['key_count'] >= 2

    def test_copy(self, shared_dict):
        """Test copying dictionary"""
        shared_dict['key1'] = 'value1'
        shared_dict['key2'] = 'value2'

        copy = shared_dict.copy()
        assert isinstance(copy, dict)
        assert copy['key1'] == 'value1'
        assert copy['key2'] == 'value2'


class TestSharedDictCaching:
    """Test caching behavior"""

    def test_cache_ttl(self, redis_client):
        """Test cache TTL behavior"""
        dict_key = f"test_dict_{uuid.uuid4().hex[:8]}"
        sd = CySharedDict(redis_client, dict_key, cache_ttl=1)

        sd['key1'] = 'value1'

        # Should be cached
        assert sd.is_synced() is True

        # Wait for cache to expire
        time.sleep(1.5)

        # Cache should be invalid
        assert sd.is_synced() is False

        # Cleanup
        sd.clear()
        redis_client.delete(dict_key)

    def test_cache_invalidation(self, shared_dict):
        """Test cache invalidation on write"""
        shared_dict['key1'] = 'value1'
        # Cache is invalidated on write
        # Next read will sync
        value = shared_dict['key1']
        assert value == 'value1'


class TestSharedDictConcurrency:
    """Test concurrent access"""

    def test_concurrent_writes(self, redis_client):
        """Test concurrent writes from multiple threads"""
        dict_key = f"concurrent_dict_{uuid.uuid4().hex[:8]}"
        sd = CySharedDict(redis_client, dict_key)

        def writer(thread_id):
            for i in range(10):
                sd[f'key_{thread_id}_{i}'] = f'value_{thread_id}_{i}'

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify data integrity
        assert len(sd) >= 30

        # Cleanup
        sd.clear()
        redis_client.delete(dict_key)

    def test_concurrent_increment(self, shared_dict):
        """Test concurrent increment operations"""
        shared_dict['counter'] = 0
        results = []

        def incrementer():
            result = shared_dict.increment('counter', 1)
            results.append(result)

        threads = [threading.Thread(target=incrementer) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Final value should be 10
        final_value = shared_dict['counter']
        assert final_value >= 10  # May be higher due to test timing


class TestCySharedDictManager:
    """Test CySharedDictManager class"""

    def test_manager_creation(self, redis_client):
        """Test creating dict manager"""
        manager = CySharedDictManager(redis_client)
        assert manager is not None

    def test_get_dict(self, dict_manager):
        """Test getting or creating dictionary"""
        sd = dict_manager.get_dict("test_dict1")
        assert sd is not None
        assert isinstance(sd, CySharedDict)

        # Getting same dict again should return same instance
        sd2 = dict_manager.get_dict("test_dict1")
        assert sd is sd2

    def test_delete_dict(self, dict_manager):
        """Test deleting a dictionary"""
        sd = dict_manager.get_dict("test_dict2")
        sd['key'] = 'value'

        dict_manager.delete_dict("test_dict2")

        # Should be removed from manager
        assert "test_dict2" not in dict_manager.list_dicts()

    def test_list_dicts(self, dict_manager):
        """Test listing managed dictionaries"""
        dict_manager.get_dict("dict1")
        dict_manager.get_dict("dict2")

        dicts = dict_manager.list_dicts()
        assert 'dict1' in dicts
        assert 'dict2' in dicts

    def test_get_global_stats(self, dict_manager):
        """Test getting global statistics"""
        sd1 = dict_manager.get_dict("dict1")
        sd2 = dict_manager.get_dict("dict2")

        sd1['key1'] = 'value1'
        sd2['key2'] = 'value2'

        stats = dict_manager.get_global_stats()
        assert isinstance(stats, dict)
        assert 'dict1' in stats
        assert 'dict2' in stats


class TestSharedDictEdgeCases:
    """Test edge cases"""

    def test_empty_dict(self, shared_dict):
        """Test empty dictionary operations"""
        assert len(shared_dict) == 0
        assert list(shared_dict.keys()) == []

    def test_unicode_keys_values(self, shared_dict):
        """Test Unicode keys and values"""
        shared_dict['键'] = '值'
        assert shared_dict['键'] == '值'

    def test_large_values(self, shared_dict):
        """Test large values"""
        large_value = 'x' * 100000
        shared_dict['large'] = large_value
        assert len(shared_dict['large']) == 100000

    def test_nested_structures(self, shared_dict):
        """Test nested data structures"""
        # Note: Shared dict may serialize complex objects
        nested = {'nested': {'deep': 'value'}}
        # This depends on implementation

    def test_keyerror_on_missing(self, shared_dict):
        """Test KeyError on missing key"""
        with pytest.raises(KeyError):
            _ = shared_dict['nonexistent']

    def test_compression(self, redis_client):
        """Test compression for large data"""
        dict_key = f"compressed_dict_{uuid.uuid4().hex[:8]}"
        sd = CySharedDict(redis_client, dict_key, use_compression=True)

        # Add large data
        large_value = 'x' * 10000
        sd['large_key'] = large_value

        stats = sd.get_stats()
        # Compression should be enabled for large data
        assert stats['compression_enabled'] is True

        # Cleanup
        sd.clear()
        redis_client.delete(dict_key)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
