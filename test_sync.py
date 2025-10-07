#!/usr/bin/env python3
"""
Simple test script to verify sync Redis functionality
"""

import sys
import os

# Add the package to path for testing
sys.path.insert(0, os.path.dirname(__file__))

def test_sync_redis():
    """Test the sync Redis client"""

    try:
        # Try to import the Cython client
        from cy_redis.core.cy_redis_client import CyRedisClient
        print("✓ Successfully imported CyRedisClient")

        # Create sync client
        client = CyRedisClient()
        print("✓ Created CyRedisClient instance")

        # Test basic operations
        print("\nTesting basic operations...")

        # Set a value
        result = client.set(b"test_key", b"test_value")
        print(f"✓ SET result: {result}")

        # Get the value
        result = client.get(b"test_key")
        print(f"✓ GET result: {result}")

        # Test increment
        result = client.incr(b"counter")
        print(f"✓ INCR result: {result}")

        result = client.incr(b"counter")
        print(f"✓ INCR result: {result}")

        # Check existence
        result = client.exists(b"test_key")
        print(f"✓ EXISTS result: {result}")

        result = client.exists(b"nonexistent")
        print(f"✓ EXISTS (nonexistent) result: {result}")

        # Delete
        result = client.delete(b"test_key")
        print(f"✓ DELETE result: {result}")

        result = client.delete(b"counter")
        print(f"✓ DELETE result: {result}")

        print("\n🎉 All sync Redis tests passed!")

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True

if __name__ == "__main__":
    print("Testing CyRedis sync functionality...")
    success = test_sync_redis()
    if success:
        print("All tests passed!")
        sys.exit(0)
    else:
        print("Tests failed!")
        sys.exit(1)
