#!/usr/bin/env python3
"""
Example usage of CyRedis Shared Dictionary
Demonstrates replicated dictionary with concurrency control
"""

import time
import threading
from shared_dict import SharedDict, SharedDictManager

def worker_thread(thread_id, shared_dict):
    """Worker thread that performs operations on shared dictionary."""
    print(f"Thread {thread_id}: Starting operations")

    # Set some values
    shared_dict[f"thread_{thread_id}_data"] = f"Data from thread {thread_id}"
    shared_dict[f"counter_{thread_id}"] = 0

    # Perform some atomic operations
    for i in range(10):
        current = shared_dict.increment(f"counter_{thread_id}")
        shared_dict[f"thread_{thread_id}_item_{i}"] = f"Item {i} from thread {thread_id}"

    print(f"Thread {thread_id}: Completed operations")

def demonstrate_shared_dict():
    """Demonstrate shared dictionary functionality."""
    print("🚀 CyRedis Shared Dictionary Demo")
    print("=" * 50)

    # Create a shared dictionary
    print("\n1. Creating shared dictionary...")
    shared_dict = SharedDict(dict_key="demo_dict")

    # Basic operations
    print("\n2. Basic dictionary operations...")
    shared_dict["name"] = "CyRedis Demo"
    shared_dict["version"] = "1.0.0"
    shared_dict["features"] = ["compression", "concurrency", "replication"]

    print(f"Name: {shared_dict['name']}")
    print(f"Version: {shared_dict['version']}")
    print(f"Features: {shared_dict['features']}")

    # Atomic operations
    print("\n3. Atomic increment operations...")
    for i in range(5):
        count = shared_dict.increment("visit_count")
        print(f"Visit count: {count}")

    # Bulk operations
    print("\n4. Bulk operations...")
    bulk_data = {
        "user_1": {"name": "Alice", "score": 100},
        "user_2": {"name": "Bob", "score": 95},
        "user_3": {"name": "Charlie", "score": 90}
    }
    shared_dict.bulk_update(bulk_data)

    print("Bulk data added:")
    for key in ["user_1", "user_2", "user_3"]:
        print(f"  {key}: {shared_dict[key]}")

    # Concurrency test
    print("\n5. Concurrency test with multiple threads...")
    threads = []
    for i in range(3):
        t = threading.Thread(target=worker_thread, args=(i, shared_dict))
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Check results
    print("\n6. Results after concurrent operations...")
    total_counters = sum(shared_dict.get(f"counter_{i}", 0) for i in range(3))
    print(f"Total counter values: {total_counters}")

    # Show some thread data
    for i in range(3):
        thread_data = shared_dict.get(f"thread_{i}_data")
        counter = shared_dict.get(f"counter_{i}", 0)
        print(f"Thread {i}: data='{thread_data}', counter={counter}")

    # Statistics
    print("\n7. Dictionary statistics...")
    stats = shared_dict.get_stats()
    print(f"Key count: {stats['key_count']}")
    print(f"Total size: {stats['total_size_bytes']} bytes")
    print(f"Compression enabled: {stats['compression_enabled']}")
    print(f"Cache TTL: {stats['cache_ttl_seconds']}s")

    # Dictionary manager
    print("\n8. Using Shared Dictionary Manager...")
    manager = SharedDictManager()

    # Create multiple dictionaries
    users_dict = manager.get_dict("users")
    config_dict = manager.get_dict("config")

    users_dict["admin"] = {"role": "administrator", "permissions": ["read", "write", "delete"]}
    config_dict["max_connections"] = 100
    config_dict["timeout"] = 30

    print(f"Managed dictionaries: {manager.list_dicts()}")
    print(f"Users dict: {dict(users_dict)}")
    print(f"Config dict: {dict(config_dict)}")

    # Global stats
    global_stats = manager.get_global_stats()
    print(f"Global stats: {len(global_stats)} dictionaries managed")

    print("\n✅ Shared Dictionary Demo Complete!")
    print("\nKey Features Demonstrated:")
    print("  ✓ Redis replication - data persists across processes")
    print("  ✓ Concurrency control - atomic operations with distributed locks")
    print("  ✓ Local caching - performance optimization")
    print("  ✓ Compression - efficient storage for large data")
    print("  ✓ Bulk operations - efficient multi-key operations")
    print("  ✓ Dictionary manager - organize multiple shared dictionaries")

if __name__ == "__main__":
    demonstrate_shared_dict()
