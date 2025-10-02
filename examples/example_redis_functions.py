#!/usr/bin/env python3
"""
Example demonstrating CyRedis Standard Library of Redis Functions
Atomic operations with one round-trip using Redis 7+ Functions
"""

import time
import threading
from production_redis import ProductionRedis

def demonstrate_function_libraries():
    """Demonstrate the Redis Functions standard library."""
    print("🚀 CyRedis Standard Library of Redis Functions")
    print("=" * 55)

    # Create production Redis client
    redis = ProductionRedis()
    print("✓ Connected to Redis with production client")

    # Initialize functions library
    try:
        from cy_redis.functions import RedisFunctions
        functions = RedisFunctions(redis._client)
        print("✓ Initialized Redis Functions library")
    except ImportError:
        print("⚠️  Redis Functions not available (requires Redis 7+)")
        return

    # Load function libraries
    print("\n📚 Loading Function Libraries...")
    try:
        results = functions.load_all_libraries()
        for lib_name, result in results.items():
            status = result.get('status', 'unknown')
            if status == 'loaded':
                print(f"✓ Loaded {lib_name} v{result.get('version', 'unknown')}")
            elif status == 'already_loaded':
                print(f"ℹ️  {lib_name} already loaded")
            else:
                print(f"✗ Failed to load {lib_name}: {result.get('error', 'unknown')}")
    except Exception as e:
        print(f"✗ Failed to load libraries: {e}")
        return

    # Demonstrate distributed locks
    demonstrate_locks(functions)

    # Demonstrate rate limiting
    demonstrate_rate_limiting(functions)

    # Demonstrate reliable queues
    demonstrate_queues(functions)

    # Show library information
    demonstrate_library_info(functions)

    redis.close()

def demonstrate_locks(functions):
    """Demonstrate distributed locks with fencing tokens."""
    print("\n🔒 Distributed Locks with Fencing Tokens")
    print("-" * 40)

    lock_key = "demo_resource"
    owner1 = "worker_1"
    owner2 = "worker_2"

    # Worker 1 acquires lock
    print("Worker 1 acquiring lock...")
    result1 = functions.locks.acquire(lock_key, owner1, ttl_ms=10000)
    print(f"  Result: {result1}")

    # Worker 2 tries to acquire (should fail)
    print("Worker 2 trying to acquire same lock...")
    result2 = functions.locks.acquire(lock_key, owner2, ttl_ms=10000)
    print(f"  Result: {result2}")

    # Worker 1 re-acquires (reentrant)
    print("Worker 1 re-acquiring (reentrant)...")
    result3 = functions.locks.acquire(lock_key, owner1, ttl_ms=10000)
    print(f"  Result: {result3}")

    # Worker 1 releases once (still held)
    print("Worker 1 releasing once...")
    released1 = functions.locks.release(lock_key, owner1)
    print(f"  Still held: {released1 == 0}")

    # Worker 1 releases again (fully released)
    print("Worker 1 releasing again...")
    released2 = functions.locks.release(lock_key, owner1)
    print(f"  Fully released: {released2 == 1}")

    # Worker 2 can now acquire
    print("Worker 2 acquiring now...")
    result4 = functions.locks.acquire(lock_key, owner2, ttl_ms=10000)
    print(f"  Result: {result4}")

    # Worker 2 releases
    functions.locks.release(lock_key, owner2)
    print("✓ Lock demonstration complete")

def demonstrate_rate_limiting(functions):
    """Demonstrate rate limiting with multiple algorithms."""
    print("\n⚡ Rate Limiting Algorithms")
    print("-" * 30)

    # Token bucket
    print("Token Bucket (100 tokens, refill 10/ms)...")
    for i in range(5):
        result = functions.rate.token_bucket("api_calls", capacity=100, refill_rate_per_ms=0.01, cost=20)
        print(f"  Request {i+1}: {result}")
        if not result['allowed']:
            print(f"    Rate limited! Retry after {result['retry_after_ms']}ms")
            break

    print("\nSliding Window (10 requests per 5000ms)...")
    for i in range(12):
        result = functions.rate.sliding_window("user_actions", window_ms=5000, max_requests=10)
        print(f"  Request {i+1}: {result}")
        if not result['allowed']:
            print(f"    Rate limited! Retry after {result['retry_after_ms']}ms")
            break

    print("✓ Rate limiting demonstration complete")

def demonstrate_queues(functions):
    """Demonstrate reliable queues with deduplication."""
    print("\n📋 Reliable Queues with Deduplication")
    print("-" * 40)

    queue_name = "demo_queue"

    # Enqueue messages
    print("Enqueueing messages...")
    messages = [
        ("msg_1", "First message"),
        ("msg_2", "Second message"),
        ("msg_1", "Duplicate message"),  # Should be deduplicated
        ("msg_3", "Third message")
    ]

    for msg_id, payload in messages:
        result = functions.queue.enqueue(queue_name, msg_id, payload)
        print(f"  {msg_id}: {result}")

    # Pull messages
    print("\nPulling messages...")
    pulled = functions.queue.pull(queue_name, visibility_ms=10000, max_messages=2)
    print(f"  Pulled {len(pulled)} messages")

    for i, msg_json in enumerate(pulled):
        print(f"  Message {i+1}: {msg_json}")

        # Parse and acknowledge
        import json
        msg = json.loads(msg_json)
        msg_id = msg['id']

        if i == 0:
            # Ack first message
            ack_result = functions.queue.ack(queue_name, msg_id)
            print(f"    Acked {msg_id}: {ack_result}")
        else:
            # Nack second message (send to DLQ)
            nack_result = functions.queue.nack(queue_name, msg_id, requeue=False)
            print(f"    Nacked {msg_id} to DLQ: {nack_result}")

    # Try to pull again
    print("\nPulling remaining messages...")
    remaining = functions.queue.pull(queue_name, visibility_ms=10000, max_messages=5)
    print(f"  Pulled {len(remaining)} more messages")

    print("✓ Queue demonstration complete")

def demonstrate_library_info(functions):
    """Show information about loaded function libraries."""
    print("\n📊 Function Library Information")
    print("-" * 35)

    libraries = functions.list_loaded_libraries()
    print(f"Loaded libraries: {libraries}")

    for lib_name in libraries[:3]:  # Show first 3
        info = functions.get_library_info(lib_name)
        if info:
            print(f"\n{lib_name} (v{info.get('version', 'unknown')}):")
            print(f"  Description: {info.get('description', 'N/A')}")
            print(f"  Functions: {', '.join(info.get('functions', []))}")

    print("✓ Library information complete")

def demonstrate_concurrent_operations():
    """Demonstrate concurrent operations using functions."""
    print("\n🔄 Concurrent Operations with Functions")
    print("-" * 40)

    from production_redis import ProductionRedis
    from cy_redis.functions import RedisFunctions

    redis = ProductionRedis()
    functions = RedisFunctions(redis._client)

    def worker_thread(thread_id):
        """Worker thread performing operations."""
        # Each thread gets its own lock
        lock_key = f"thread_{thread_id}"

        # Acquire lock
        result = functions.locks.acquire(lock_key, f"thread_{thread_id}", ttl_ms=5000)
        if result['acquired']:
            print(f"Thread {thread_id}: Acquired lock with token {result['fencing_token']}")

            # Do some rate limited operations
            for i in range(3):
                rate_result = functions.rate.token_bucket(
                    f"thread_{thread_id}_rate",
                    capacity=5,
                    refill_rate_per_ms=0.1,
                    cost=1
                )
                if rate_result['allowed']:
                    print(f"Thread {thread_id}: Operation {i+1} allowed")
                else:
                    print(f"Thread {thread_id}: Operation {i+1} rate limited")

            # Release lock
            functions.locks.release(lock_key, f"thread_{thread_id}")
            print(f"Thread {thread_id}: Released lock")
        else:
            print(f"Thread {thread_id}: Failed to acquire lock")

    # Start concurrent threads
    threads = []
    for i in range(3):
        t = threading.Thread(target=worker_thread, args=(i,))
        threads.append(t)
        t.start()

    # Wait for completion
    for t in threads:
        t.join()

    redis.close()
    print("✓ Concurrent operations demonstration complete")

if __name__ == "__main__":
    try:
        demonstrate_function_libraries()
        print("\n" + "="*55)
        demonstrate_concurrent_operations()
    except Exception as e:
        print(f"❌ Demonstration failed: {e}")
        import traceback
        traceback.print_exc()
