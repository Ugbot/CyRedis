#!/usr/bin/env python3
"""
Test script to validate the async Redis design
"""

import asyncio
import sys
import os

# Add the package to path for testing
sys.path.insert(0, os.path.dirname(__file__))

async def test_async_design():
    """Test the async Redis client design"""

    try:
        from cy_redis.core.async_core import AsyncRedisClient, create_async_client
        print("✓ Successfully imported AsyncRedisClient and create_async_client")

        # Test 1: Direct client creation and context manager
        print("\nTesting direct client creation...")
        async with AsyncRedisClient(host="localhost", port=6379) as client:
            print("✓ Created AsyncRedisClient with context manager")

            # Test ping
            result = await client.ping()
            print(f"✓ PING result: {result}")

            # Test set/get
            await client.set("test_key", "test_value")
            result = await client.get("test_key")
            print(f"✓ SET/GET result: {result}")

            # Test incr
            result = await client.incr("counter")
            print(f"✓ INCR result: {result}")

            result = await client.incr("counter")
            print(f"✓ INCR result: {result}")

            # Test exists
            result = await client.exists("test_key")
            print(f"✓ EXISTS result: {result}")

            result = await client.exists("nonexistent")
            print(f"✓ EXISTS (nonexistent) result: {result}")

        # Test 2: Convenience function
        print("\nTesting convenience function...")
        client2 = await create_async_client(host="localhost", port=6379)
        try:
            result = await client2.info("server")
            print(f"✓ INFO result: {result[:50]}..." if result else "✓ INFO result: None")
        finally:
            await client2.close()

        # Test 3: Hash operations
        print("\nTesting hash operations...")
        async with AsyncRedisClient() as client:
            await client.hset("test_hash", "field1", "value1")
            await client.hset("test_hash", "field2", "value2")

            result = await client.hget("test_hash", "field1")
            print(f"✓ HGET result: {result}")

            result = await client.hgetall("test_hash")
            print(f"✓ HGETALL result: {result}")

        # Test 4: List operations
        print("\nTesting list operations...")
        async with AsyncRedisClient() as client:
            await client.rpush("test_list", "item1", "item2", "item3")

            result = await client.lpop("test_list")
            print(f"✓ LPOP result: {result}")

            result = await client.rpop("test_list")
            print(f"✓ RPOP result: {result}")

        # Test 5: Set operations
        print("\nTesting set operations...")
        async with AsyncRedisClient() as client:
            await client.sadd("test_set", "member1", "member2", "member3")

            result = await client.smembers("test_set")
            print(f"✓ SMEMBERS result: {result}")

            await client.srem("test_set", "member2")
            result = await client.smembers("test_set")
            print(f"✓ SMEMBERS after SREM result: {result}")

        # Cleanup
        print("\nCleaning up...")
        async with AsyncRedisClient() as client:
            await client.delete(["test_key", "counter", "test_hash", "test_list", "test_set"])

        print("\n🎉 All async Redis design tests passed!")

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True

if __name__ == "__main__":
    print("Testing CyRedis async design...")

    # Check if uvloop is available
    try:
        import uvloop
        print("✓ uvloop is available")
    except ImportError:
        print("⚠️  uvloop not available - using standard asyncio")

    success = asyncio.run(test_async_design())
    if success:
        print("All tests passed!")
        sys.exit(0)
    else:
        print("Tests failed!")
        sys.exit(1)
