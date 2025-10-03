"""
Integration tests for basic Redis operations.

Tests fundamental Redis commands end-to-end including:
- String operations (GET/SET/DEL)
- Hash operations (HGET/HSET/HDEL)
- List operations (LPUSH/RPUSH/LPOP/RPOP)
- Set operations (SADD/SREM/SMEMBERS)
- Sorted set operations (ZADD/ZREM/ZRANGE)
- Key expiration and TTL
- Transaction operations
"""

import pytest
import time


@pytest.mark.integration
class TestBasicStringOperations:
    """Test basic string operations."""

    def test_set_get(self, redis_client, unique_key):
        """Test SET and GET operations."""
        key = f"{unique_key}:string"
        value = "test_value"

        # Set value
        result = redis_client.set(key, value)
        assert result is True

        # Get value
        retrieved = redis_client.get(key)
        assert retrieved == value

        # Cleanup
        redis_client.delete(key)

    def test_set_with_expiration(self, redis_client, unique_key):
        """Test SET with expiration."""
        key = f"{unique_key}:expire"
        value = "expiring_value"

        # Set with 2 second expiration
        redis_client.set(key, value, ex=2)

        # Value should exist
        assert redis_client.get(key) == value

        # Wait for expiration
        time.sleep(2.1)

        # Value should be gone
        assert redis_client.get(key) is None

    def test_set_nx(self, redis_client, unique_key):
        """Test SET NX (set if not exists)."""
        key = f"{unique_key}:nx"

        # First set should succeed
        result = redis_client.set(key, "value1", nx=True)
        assert result is True

        # Second set should fail
        result = redis_client.set(key, "value2", nx=True)
        assert result is None

        # Value should still be first value
        assert redis_client.get(key) == "value1"

        # Cleanup
        redis_client.delete(key)

    def test_incr_decr(self, redis_client, unique_key):
        """Test INCR and DECR operations."""
        key = f"{unique_key}:counter"

        # Increment from 0
        result = redis_client.incr(key)
        assert result == 1

        # Increment by 5
        result = redis_client.incrby(key, 5)
        assert result == 6

        # Decrement
        result = redis_client.decr(key)
        assert result == 5

        # Decrement by 3
        result = redis_client.decrby(key, 3)
        assert result == 2

        # Cleanup
        redis_client.delete(key)

    def test_mget_mset(self, redis_client, unique_key):
        """Test MGET and MSET operations."""
        keys = [f"{unique_key}:multi:{i}" for i in range(5)]
        values = [f"value_{i}" for i in range(5)]

        # Multi-set
        mapping = {k: v for k, v in zip(keys, values)}
        redis_client.mset(mapping)

        # Multi-get
        retrieved = redis_client.mget(keys)
        assert retrieved == values

        # Cleanup
        redis_client.delete(*keys)


@pytest.mark.integration
class TestHashOperations:
    """Test hash operations."""

    def test_hset_hget(self, redis_client, unique_key):
        """Test HSET and HGET operations."""
        key = f"{unique_key}:hash"

        # Set hash fields
        redis_client.hset(key, "field1", "value1")
        redis_client.hset(key, "field2", "value2")

        # Get individual fields
        assert redis_client.hget(key, "field1") == "value1"
        assert redis_client.hget(key, "field2") == "value2"

        # Cleanup
        redis_client.delete(key)

    def test_hmset_hmget(self, redis_client, unique_key):
        """Test HMSET and HMGET operations."""
        key = f"{unique_key}:hash"
        mapping = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }

        # Multi-set
        redis_client.hset(key, mapping=mapping)

        # Multi-get
        values = redis_client.hmget(key, ["field1", "field2", "field3"])
        assert values == ["value1", "value2", "value3"]

        # Cleanup
        redis_client.delete(key)

    def test_hgetall(self, redis_client, unique_key):
        """Test HGETALL operation."""
        key = f"{unique_key}:hash"
        mapping = {"a": "1", "b": "2", "c": "3"}

        redis_client.hset(key, mapping=mapping)

        # Get all
        result = redis_client.hgetall(key)
        assert result == mapping

        # Cleanup
        redis_client.delete(key)

    def test_hincrby(self, redis_client, unique_key):
        """Test HINCRBY operation."""
        key = f"{unique_key}:hash"

        # Increment from 0
        result = redis_client.hincrby(key, "counter", 1)
        assert result == 1

        # Increment by 10
        result = redis_client.hincrby(key, "counter", 10)
        assert result == 11

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestListOperations:
    """Test list operations."""

    def test_lpush_lpop(self, redis_client, unique_key):
        """Test LPUSH and LPOP operations."""
        key = f"{unique_key}:list"

        # Push items
        redis_client.lpush(key, "item1", "item2", "item3")

        # Pop items (LIFO order)
        assert redis_client.lpop(key) == "item3"
        assert redis_client.lpop(key) == "item2"
        assert redis_client.lpop(key) == "item1"
        assert redis_client.lpop(key) is None

    def test_rpush_rpop(self, redis_client, unique_key):
        """Test RPUSH and RPOP operations."""
        key = f"{unique_key}:list"

        # Push items
        redis_client.rpush(key, "item1", "item2", "item3")

        # Pop items (FIFO order from right)
        assert redis_client.rpop(key) == "item3"
        assert redis_client.rpop(key) == "item2"
        assert redis_client.rpop(key) == "item1"

        # Cleanup
        redis_client.delete(key)

    def test_lrange(self, redis_client, unique_key):
        """Test LRANGE operation."""
        key = f"{unique_key}:list"

        # Push items
        redis_client.rpush(key, "a", "b", "c", "d", "e")

        # Get range
        assert redis_client.lrange(key, 0, 2) == ["a", "b", "c"]
        assert redis_client.lrange(key, 1, 3) == ["b", "c", "d"]
        assert redis_client.lrange(key, 0, -1) == ["a", "b", "c", "d", "e"]

        # Cleanup
        redis_client.delete(key)

    def test_llen(self, redis_client, unique_key):
        """Test LLEN operation."""
        key = f"{unique_key}:list"

        # Empty list
        assert redis_client.llen(key) == 0

        # Push items
        redis_client.rpush(key, "a", "b", "c")
        assert redis_client.llen(key) == 3

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestSetOperations:
    """Test set operations."""

    def test_sadd_smembers(self, redis_client, unique_key):
        """Test SADD and SMEMBERS operations."""
        key = f"{unique_key}:set"

        # Add members
        redis_client.sadd(key, "a", "b", "c")

        # Get members
        members = redis_client.smembers(key)
        assert members == {"a", "b", "c"}

        # Cleanup
        redis_client.delete(key)

    def test_sismember(self, redis_client, unique_key):
        """Test SISMEMBER operation."""
        key = f"{unique_key}:set"

        redis_client.sadd(key, "a", "b", "c")

        # Check membership
        assert redis_client.sismember(key, "a") is True
        assert redis_client.sismember(key, "d") is False

        # Cleanup
        redis_client.delete(key)

    def test_srem(self, redis_client, unique_key):
        """Test SREM operation."""
        key = f"{unique_key}:set"

        redis_client.sadd(key, "a", "b", "c")

        # Remove member
        result = redis_client.srem(key, "b")
        assert result == 1

        # Check remaining members
        assert redis_client.smembers(key) == {"a", "c"}

        # Cleanup
        redis_client.delete(key)

    def test_sunion(self, redis_client, unique_key):
        """Test SUNION operation."""
        key1 = f"{unique_key}:set1"
        key2 = f"{unique_key}:set2"

        redis_client.sadd(key1, "a", "b", "c")
        redis_client.sadd(key2, "c", "d", "e")

        # Union
        result = redis_client.sunion(key1, key2)
        assert result == {"a", "b", "c", "d", "e"}

        # Cleanup
        redis_client.delete(key1, key2)


@pytest.mark.integration
class TestSortedSetOperations:
    """Test sorted set operations."""

    def test_zadd_zrange(self, redis_client, unique_key):
        """Test ZADD and ZRANGE operations."""
        key = f"{unique_key}:zset"

        # Add members with scores
        redis_client.zadd(key, {"a": 1, "b": 2, "c": 3})

        # Get range
        result = redis_client.zrange(key, 0, -1)
        assert result == ["a", "b", "c"]

        # Get range with scores
        result = redis_client.zrange(key, 0, -1, withscores=True)
        assert result == [("a", 1.0), ("b", 2.0), ("c", 3.0)]

        # Cleanup
        redis_client.delete(key)

    def test_zincrby(self, redis_client, unique_key):
        """Test ZINCRBY operation."""
        key = f"{unique_key}:zset"

        redis_client.zadd(key, {"member": 10})

        # Increment score
        result = redis_client.zincrby(key, 5, "member")
        assert result == 15.0

        # Cleanup
        redis_client.delete(key)

    def test_zrem(self, redis_client, unique_key):
        """Test ZREM operation."""
        key = f"{unique_key}:zset"

        redis_client.zadd(key, {"a": 1, "b": 2, "c": 3})

        # Remove member
        result = redis_client.zrem(key, "b")
        assert result == 1

        # Check remaining members
        assert redis_client.zrange(key, 0, -1) == ["a", "c"]

        # Cleanup
        redis_client.delete(key)

    def test_zrangebyscore(self, redis_client, unique_key):
        """Test ZRANGEBYSCORE operation."""
        key = f"{unique_key}:zset"

        redis_client.zadd(key, {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

        # Get members by score range
        result = redis_client.zrangebyscore(key, 2, 4)
        assert result == ["b", "c", "d"]

        # Cleanup
        redis_client.delete(key)


@pytest.mark.integration
class TestKeyOperations:
    """Test key management operations."""

    def test_exists(self, redis_client, unique_key):
        """Test EXISTS operation."""
        key = f"{unique_key}:exists"

        # Key doesn't exist
        assert redis_client.exists(key) == 0

        # Create key
        redis_client.set(key, "value")
        assert redis_client.exists(key) == 1

        # Cleanup
        redis_client.delete(key)

    def test_delete(self, redis_client, unique_key):
        """Test DELETE operation."""
        key = f"{unique_key}:delete"

        redis_client.set(key, "value")

        # Delete key
        result = redis_client.delete(key)
        assert result == 1

        # Key should not exist
        assert redis_client.exists(key) == 0

    def test_expire_ttl(self, redis_client, unique_key):
        """Test EXPIRE and TTL operations."""
        key = f"{unique_key}:ttl"

        redis_client.set(key, "value")

        # Set expiration
        redis_client.expire(key, 10)

        # Check TTL
        ttl = redis_client.ttl(key)
        assert 8 <= ttl <= 10

        # Cleanup
        redis_client.delete(key)

    def test_rename(self, redis_client, unique_key):
        """Test RENAME operation."""
        old_key = f"{unique_key}:old"
        new_key = f"{unique_key}:new"

        redis_client.set(old_key, "value")

        # Rename
        redis_client.rename(old_key, new_key)

        # Old key should not exist
        assert redis_client.exists(old_key) == 0

        # New key should have the value
        assert redis_client.get(new_key) == "value"

        # Cleanup
        redis_client.delete(new_key)

    def test_type(self, redis_client, unique_key):
        """Test TYPE operation."""
        string_key = f"{unique_key}:string"
        list_key = f"{unique_key}:list"
        hash_key = f"{unique_key}:hash"

        redis_client.set(string_key, "value")
        redis_client.lpush(list_key, "item")
        redis_client.hset(hash_key, "field", "value")

        # Check types
        assert redis_client.type(string_key) == "string"
        assert redis_client.type(list_key) == "list"
        assert redis_client.type(hash_key) == "hash"

        # Cleanup
        redis_client.delete(string_key, list_key, hash_key)


@pytest.mark.integration
class TestTransactions:
    """Test transaction operations."""

    def test_pipeline(self, redis_client, unique_key):
        """Test pipeline operations."""
        key1 = f"{unique_key}:pipe1"
        key2 = f"{unique_key}:pipe2"

        # Execute pipeline
        pipe = redis_client.pipeline()
        pipe.set(key1, "value1")
        pipe.set(key2, "value2")
        pipe.get(key1)
        pipe.get(key2)
        results = pipe.execute()

        # Check results
        assert results == [True, True, "value1", "value2"]

        # Cleanup
        redis_client.delete(key1, key2)

    def test_watch(self, redis_client, unique_key):
        """Test WATCH operation."""
        key = f"{unique_key}:watch"

        redis_client.set(key, "0")

        # Watch key
        pipe = redis_client.pipeline()
        pipe.watch(key)

        # Get current value
        current = int(redis_client.get(key))

        # Execute transaction
        pipe.multi()
        pipe.set(key, str(current + 1))
        results = pipe.execute()

        # Should succeed
        assert results == [True]

        # Check final value
        assert redis_client.get(key) == "1"

        # Cleanup
        redis_client.delete(key)
