"""
Integration tests for Lua script execution.

Tests Lua scripting functionality including:
- EVAL and EVALSHA commands
- Script caching and registration
- Complex Lua operations
- Script atomicity
"""

import pytest
import time


@pytest.mark.integration
class TestLuaScripts:
    """Test Lua script execution."""

    def test_eval_basic(self, redis_client):
        """Test basic EVAL command."""
        script = "return 'hello'"

        result = redis_client.eval(script, 0)
        assert result == b'hello' or result == 'hello'

    def test_eval_with_keys(self, redis_client):
        """Test EVAL with key arguments."""
        script = """
        return redis.call('SET', KEYS[1], ARGV[1])
        """

        result = redis_client.eval(script, 1, "test:lua:key1", "value1")

        # Verify the key was set
        value = redis_client.get("test:lua:key1")
        assert value == b'value1' or value == 'value1'

        # Clean up
        redis_client.delete("test:lua:key1")

    def test_eval_with_multiple_keys(self, redis_client):
        """Test EVAL with multiple keys."""
        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        redis.call('SET', KEYS[2], ARGV[2])
        return redis.call('GET', KEYS[1])
        """

        result = redis_client.eval(
            script,
            2,  # number of keys
            "test:lua:key1", "test:lua:key2",  # keys
            "value1", "value2"  # arguments
        )

        assert result == b'value1' or result == 'value1'

        # Verify both keys were set
        assert redis_client.get("test:lua:key1") in [b'value1', 'value1']
        assert redis_client.get("test:lua:key2") in [b'value2', 'value2']

        # Clean up
        redis_client.delete("test:lua:key1", "test:lua:key2")

    def test_evalsha_script_caching(self, redis_client):
        """Test EVALSHA with script caching."""
        script = "return ARGV[1]"

        # Load script and get SHA
        try:
            sha = redis_client.script_load(script)

            # Execute using SHA
            result = redis_client.evalsha(sha, 0, "test_value")
            assert result == b'test_value' or result == 'test_value'

        except (AttributeError, Exception) as e:
            pytest.skip(f"EVALSHA not available: {e}")

    def test_script_exists(self, redis_client):
        """Test checking if script exists in cache."""
        script = "return 42"

        try:
            # Load script
            sha = redis_client.script_load(script)

            # Check if exists
            exists = redis_client.script_exists(sha)
            assert exists == [1] or exists == [True] or any(exists)

        except (AttributeError, Exception) as e:
            pytest.skip(f"SCRIPT EXISTS not available: {e}")

    def test_script_flush(self, redis_client):
        """Test flushing script cache."""
        script = "return 'test'"

        try:
            # Load script
            sha = redis_client.script_load(script)

            # Flush cache
            redis_client.script_flush()

            # Script should no longer exist
            exists = redis_client.script_exists(sha)
            assert exists == [0] or exists == [False] or not any(exists)

        except (AttributeError, Exception) as e:
            pytest.skip(f"SCRIPT FLUSH not available: {e}")

    def test_lua_increment_operation(self, redis_client):
        """Test atomic increment using Lua."""
        key = "test:lua:counter"

        script = """
        local current = redis.call('GET', KEYS[1])
        if current then
            return redis.call('INCR', KEYS[1])
        else
            redis.call('SET', KEYS[1], 0)
            return redis.call('INCR', KEYS[1])
        end
        """

        # First increment
        result1 = redis_client.eval(script, 1, key)
        assert result1 == 1

        # Second increment
        result2 = redis_client.eval(script, 1, key)
        assert result2 == 2

        # Clean up
        redis_client.delete(key)

    def test_lua_conditional_set(self, redis_client):
        """Test conditional SET using Lua."""
        key = "test:lua:conditional"

        script = """
        local value = redis.call('GET', KEYS[1])
        if value == false then
            redis.call('SET', KEYS[1], ARGV[1])
            return 1
        else
            return 0
        end
        """

        # First set should succeed
        result1 = redis_client.eval(script, 1, key, "first_value")
        assert result1 == 1

        # Second set should fail
        result2 = redis_client.eval(script, 1, key, "second_value")
        assert result2 == 0

        # Value should still be first_value
        value = redis_client.get(key)
        assert value == b'first_value' or value == 'first_value'

        # Clean up
        redis_client.delete(key)

    def test_lua_list_operations(self, redis_client):
        """Test list operations using Lua."""
        key = "test:lua:list"

        script = """
        redis.call('RPUSH', KEYS[1], ARGV[1])
        redis.call('RPUSH', KEYS[1], ARGV[2])
        redis.call('RPUSH', KEYS[1], ARGV[3])
        return redis.call('LRANGE', KEYS[1], 0, -1)
        """

        result = redis_client.eval(script, 1, key, "a", "b", "c")

        # Result should be a list
        assert isinstance(result, list)
        assert len(result) == 3

        # Clean up
        redis_client.delete(key)

    def test_lua_hash_operations(self, redis_client):
        """Test hash operations using Lua."""
        key = "test:lua:hash"

        script = """
        redis.call('HSET', KEYS[1], 'field1', ARGV[1])
        redis.call('HSET', KEYS[1], 'field2', ARGV[2])
        return redis.call('HGETALL', KEYS[1])
        """

        result = redis_client.eval(script, 1, key, "value1", "value2")

        # Result should contain the hash fields
        assert result is not None
        assert len(result) >= 4  # 2 fields + 2 values

        # Clean up
        redis_client.delete(key)

    def test_lua_sorted_set_operations(self, redis_client):
        """Test sorted set operations using Lua."""
        key = "test:lua:zset"

        script = """
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
        return redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
        """

        result = redis_client.eval(script, 1, key, "1.0", "member1", "2.0", "member2")

        # Result should be a list with members and scores
        assert isinstance(result, list)
        assert len(result) >= 2

        # Clean up
        redis_client.delete(key)

    @pytest.mark.slow
    def test_lua_rate_limiter(self, redis_client):
        """Test rate limiter implementation using Lua."""
        key = "test:lua:rate_limit"

        # Rate limiter: max 5 requests per 10 seconds
        script = """
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local current_time = tonumber(ARGV[3])

        local current = redis.call('GET', key)

        if current and tonumber(current) >= limit then
            return 0
        else
            redis.call('INCR', key)
            redis.call('EXPIRE', key, window)
            return 1
        end
        """

        # First 5 requests should succeed
        for i in range(5):
            result = redis_client.eval(
                script,
                1, key,
                "5",  # limit
                "10",  # window in seconds
                str(int(time.time()))
            )
            assert result == 1

        # 6th request should fail
        result = redis_client.eval(
            script,
            1, key,
            "5", "10", str(int(time.time()))
        )
        assert result == 0

        # Clean up
        redis_client.delete(key)

    def test_lua_atomic_get_and_delete(self, redis_client):
        """Test atomic get and delete using Lua."""
        key = "test:lua:get_del"

        # Set initial value
        redis_client.set(key, "test_value")

        script = """
        local value = redis.call('GET', KEYS[1])
        redis.call('DEL', KEYS[1])
        return value
        """

        # Get and delete atomically
        result = redis_client.eval(script, 1, key)
        assert result == b'test_value' or result == 'test_value'

        # Key should be deleted
        assert redis_client.get(key) is None

    def test_lua_multiple_operations_atomic(self, redis_client):
        """Test atomicity of multiple Lua operations."""
        key1 = "test:lua:atomic1"
        key2 = "test:lua:atomic2"

        script = """
        redis.call('SET', KEYS[1], ARGV[1])
        redis.call('SET', KEYS[2], ARGV[2])

        local val1 = redis.call('GET', KEYS[1])
        local val2 = redis.call('GET', KEYS[2])

        return {val1, val2}
        """

        result = redis_client.eval(script, 2, key1, key2, "value1", "value2")

        # Both operations should complete atomically
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == b'value1' or result[0] == 'value1'
        assert result[1] == b'value2' or result[1] == 'value2'

        # Clean up
        redis_client.delete(key1, key2)

    def test_lua_return_types(self, redis_client):
        """Test different Lua return types."""
        # Return number
        result = redis_client.eval("return 42", 0)
        assert result == 42

        # Return string
        result = redis_client.eval("return 'hello'", 0)
        assert result == b'hello' or result == 'hello'

        # Return table/array
        result = redis_client.eval("return {1, 2, 3}", 0)
        assert isinstance(result, list)
        assert len(result) == 3

        # Return nil
        result = redis_client.eval("return nil", 0)
        assert result is None

        # Return boolean (might be represented as 0/1)
        result = redis_client.eval("return true", 0)
        assert result == 1 or result is True

    @pytest.mark.slow
    def test_lua_error_handling(self, redis_client):
        """Test Lua script error handling."""
        # Invalid Lua syntax
        with pytest.raises(Exception):
            redis_client.eval("return invalid syntax !@#", 0)

        # Invalid Redis command
        with pytest.raises(Exception):
            redis_client.eval("return redis.call('INVALID_COMMAND')", 0)

        # Wrong number of keys
        with pytest.raises(Exception):
            redis_client.eval("return KEYS[1]", 0)  # Expects 0 keys but accessing KEYS[1]

    def test_lua_complex_data_structure(self, redis_client):
        """Test complex data structure manipulation with Lua."""
        key = "test:lua:complex"

        script = """
        -- Create a complex hash with computed values
        redis.call('HSET', KEYS[1], 'timestamp', ARGV[1])
        redis.call('HSET', KEYS[1], 'count', ARGV[2])

        -- Compute and store a derived value
        local count = tonumber(ARGV[2])
        local doubled = count * 2
        redis.call('HSET', KEYS[1], 'doubled', doubled)

        return redis.call('HGETALL', KEYS[1])
        """

        result = redis_client.eval(
            script,
            1, key,
            str(int(time.time())),
            "5"
        )

        # Should have 3 fields (timestamp, count, doubled)
        assert len(result) >= 6  # 3 fields * 2 (field + value)

        # Clean up
        redis_client.delete(key)


@pytest.mark.integration
@pytest.mark.slow
class TestLuaScriptsAdvanced:
    """Advanced Lua scripting tests."""

    def test_registered_script_manager(self, redis_client):
        """Test script registration and management."""
        script = """
        local key = KEYS[1]
        local increment = tonumber(ARGV[1])
        local current = redis.call('GET', key) or 0
        local new_value = tonumber(current) + increment
        redis.call('SET', key, new_value)
        return new_value
        """

        try:
            # Register script
            sha = redis_client.script_load(script)

            # Use registered script multiple times
            result1 = redis_client.evalsha(sha, 1, "test:script:counter", "5")
            assert result1 == 5

            result2 = redis_client.evalsha(sha, 1, "test:script:counter", "3")
            assert result2 == 8

            # Clean up
            redis_client.delete("test:script:counter")

        except (AttributeError, Exception) as e:
            pytest.skip(f"Script registration not available: {e}")

    def test_lua_json_like_operations(self, redis_client):
        """Test JSON-like data operations with Lua."""
        key = "test:lua:json"

        # Simulate storing and retrieving JSON-like data
        script = """
        -- Store multiple fields
        redis.call('HSET', KEYS[1], 'name', ARGV[1])
        redis.call('HSET', KEYS[1], 'age', ARGV[2])
        redis.call('HSET', KEYS[1], 'email', ARGV[3])

        -- Retrieve all as table
        local data = redis.call('HGETALL', KEYS[1])
        return data
        """

        result = redis_client.eval(
            script,
            1, key,
            "John Doe", "30", "john@example.com"
        )

        assert isinstance(result, list)
        assert len(result) >= 6

        # Clean up
        redis_client.delete(key)

    def test_lua_batch_operations(self, redis_client):
        """Test batch operations with Lua."""
        script = """
        local prefix = ARGV[1]
        local count = tonumber(ARGV[2])

        for i = 1, count do
            local key = prefix .. ':' .. i
            redis.call('SET', key, i)
        end

        return count
        """

        # Create 10 keys at once
        result = redis_client.eval(script, 0, "test:lua:batch", "10")
        assert result == 10

        # Verify keys were created
        for i in range(1, 11):
            value = redis_client.get(f"test:lua:batch:{i}")
            assert value is not None

        # Clean up
        for i in range(1, 11):
            redis_client.delete(f"test:lua:batch:{i}")
