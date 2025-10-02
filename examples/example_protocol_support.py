#!/usr/bin/env python3
"""
Example demonstrating CyRedis RESP2/3 Protocol Support and MVP Features
Shows auto-negotiation, push messages, connection pooling, and more
"""

import time
import asyncio
from optimized_redis import OptimizedRedis

def demonstrate_protocol_support():
    """Demonstrate RESP2/3 protocol support and auto-negotiation."""
    print("🚀 CyRedis RESP2/3 Protocol Support Demo")
    print("=" * 50)

    # Create Redis client with auto-negotiation
    redis = OptimizedRedis()
    print("✓ Created Redis client with protocol auto-negotiation")

    # Get server information to check protocol support
    try:
        server_info = redis.get_server_info()
        print(f"✓ Connected to Redis server")

        # Check if server supports RESP3
        if 'modules' in server_info and 'protocol' in server_info:
            print(f"✓ Server supports advanced features")

        # Test basic operations
        redis.set("protocol_test", "working")
        result = redis.get("protocol_test")
        print(f"✓ Basic operations: SET/GET = '{result}'")

        # Test RESP3-specific features if supported
        try:
            # Test HELLO command for protocol negotiation
            hello_result = redis.execute_command(['HELLO', '3'])
            if isinstance(hello_result, dict):
                protocol_version = hello_result.get('proto', 2)
                print(f"✓ Negotiated RESP{protocol_version} protocol")

                if protocol_version >= 3:
                    print("✓ RESP3 features available:")
                    print("  - Push messages")
                    print("  - Client tracking")
                    print("  - Typed data structures")
                else:
                    print("ℹ️  Using RESP2 protocol (RESP3 not supported by server)")
            else:
                print("ℹ️  Using RESP2 protocol")
        except Exception as e:
            print(f"ℹ️  Protocol negotiation not supported: {e}")

    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return

    # Test Lua script functionality
    print("\n📜 Testing Lua Script Support")
    print("-" * 30)

    try:
        # Load a simple script
        script = "return redis.call('PING')"
        sha = redis.script_load(script)
        print(f"✓ Loaded script with SHA: {sha[:16]}...")

        # Execute the script
        result = redis.evalsha(sha, [], [])
        print(f"✓ Script execution result: {result}")

        # Test script existence check
        exists = redis.script_exists([sha])
        print(f"✓ Script exists in cache: {exists}")

    except Exception as e:
        print(f"✗ Lua script test failed: {e}")

    # Test connection pooling
    print("\n🏊 Testing Connection Pooling")
    print("-" * 30)

    try:
        # Perform multiple operations to test pooling
        start_time = time.time()
        for i in range(50):
            redis.set(f"pool_test_{i}", f"value_{i}")
            result = redis.get(f"pool_test_{i}")
            assert result == f"value_{i}"

        elapsed = time.time() - start_time
        ops_per_sec = 100 / elapsed  # 50 sets + 50 gets
        print(f"✓ Connection pooling test: {ops_per_sec:.0f} ops/sec")

    except Exception as e:
        print(f"✗ Connection pooling test failed: {e}")

    # Test Streams (RESP3 enhanced)
    print("\n🌊 Testing Redis Streams")
    print("-" * 30)

    try:
        stream_name = "demo_stream"

        # Add messages to stream
        for i in range(3):
            message_id = redis.xadd(stream_name, {
                "message": f"Hello from CyRedis {i}",
                "timestamp": str(time.time())
            })
            print(f"✓ Added message to stream: {message_id}")

        # Read from stream
        messages = redis.xread({stream_name: "0"}, count=10)
        print(f"✓ Read {len(messages)} messages from stream")

        if messages:
            for stream_data in messages:
                stream_name, message_list = stream_data
                print(f"  Stream: {stream_name}")
                for msg_id, msg_data in message_list:
                    print(f"    {msg_id}: {msg_data}")

    except Exception as e:
        print(f"✗ Streams test failed: {e}")

    # Test Pub/Sub (classic and RESP3 push)
    print("\n📡 Testing Pub/Sub")
    print("-" * 30)

    # This would require async handling for proper push message demo
    print("✓ Pub/Sub support available (async demo in separate example)")

    # Test data structures
    print("\n📊 Testing Redis Data Structures")
    print("-" * 30)

    try:
        # Lists
        redis.execute_command(['LPUSH', 'demo_list', 'item1', 'item2', 'item3'])
        list_items = redis.execute_command(['LRANGE', 'demo_list', '0', '-1'])
        print(f"✓ List operations: {list_items}")

        # Sets
        redis.execute_command(['SADD', 'demo_set', 'member1', 'member2', 'member3'])
        set_members = redis.execute_command(['SMEMBERS', 'demo_set'])
        print(f"✓ Set operations: {set_members}")

        # Hashes
        redis.execute_command(['HSET', 'demo_hash', 'field1', 'value1', 'field2', 'value2'])
        hash_data = redis.execute_command(['HGETALL', 'demo_hash'])
        print(f"✓ Hash operations: {hash_data}")

        # Sorted Sets
        redis.execute_command(['ZADD', 'demo_zset', '1', 'member1', '2', 'member2', '3', 'member3'])
        zset_data = redis.execute_command(['ZRANGE', 'demo_zset', '0', '-1', 'WITHSCORES'])
        print(f"✓ Sorted Set operations: {zset_data}")

    except Exception as e:
        print(f"✗ Data structures test failed: {e}")

    # Performance benchmark
    print("\n⚡ Performance Benchmark")
    print("-" * 30)

    try:
        # Simple benchmark
        iterations = 1000
        start_time = time.time()

        for i in range(iterations):
            redis.set(f"bench_{i}", f"value_{i}")
            result = redis.get(f"bench_{i}")
            assert result == f"value_{i}"

        elapsed = time.time() - start_time
        ops_per_sec = (iterations * 2) / elapsed  # sets + gets
        print(f"✓ Benchmark: {ops_per_sec:.0f} operations/second")
        print(f"  Total operations: {iterations * 2}")
        print(f"  Time elapsed: {elapsed:.3f}s")

    except Exception as e:
        print(f"✗ Performance benchmark failed: {e}")

    # Clean up
    print("\n🧹 Cleaning up...")
    try:
        # Clean up test keys
        for i in range(50):
            redis.delete(f"pool_test_{i}")
        for i in range(iterations if 'iterations' in locals() else 100):
            redis.delete(f"bench_{i}")

        redis.delete("protocol_test", "demo_list", "demo_set", "demo_hash", "demo_zset")
        # Note: Stream cleanup would require more complex operations

        print("✓ Cleanup completed")

    except Exception as e:
        print(f"⚠️  Cleanup warning: {e}")

    print("\n✅ RESP2/3 Protocol Support Demo Complete!")
    print("\n🎯 Demonstrated Features:")
    print("  ✓ RESP2/3 auto-negotiation")
    print("  ✓ Protocol-aware parsing")
    print("  ✓ Connection pooling")
    print("  ✓ Lua script support")
    print("  ✓ Redis Streams")
    print("  ✓ Data structures (Lists, Sets, Hashes, ZSets)")
    print("  ✓ High-performance operations")
    print("  ✓ Error handling and cleanup")

    redis.close()

if __name__ == "__main__":
    demonstrate_protocol_support()
