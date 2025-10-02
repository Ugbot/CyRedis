#!/usr/bin/env python3
"""
Example usage of CyRedis Optimized Lua Script Management
Demonstrates high-performance script loading, caching, and execution
"""

import time
import os
from optimized_redis import OptimizedRedis
from optimized_lua_script_manager import OptimizedLuaScriptManager

def demonstrate_lua_script_management():
    """Demonstrate comprehensive Lua script management."""
    print("🚀 CyRedis Lua Script Management Demo")
    print("=" * 50)

    # Initialize Redis client
    redis = OptimizedRedis()
    print("✓ Connected to Redis")

    # Initialize optimized script manager
    script_manager = OptimizedLuaScriptManager(redis, namespace="demo:scripts")
    print("✓ Initialized optimized script manager")

    # Load scripts from the lua_scripts directory
    script_dir = "lua_scripts"
    loaded_scripts = {}

    print(f"\n📜 Loading scripts from {script_dir}/...")
    for filename in os.listdir(script_dir):
        if filename.endswith('.lua'):
            script_name = filename[:-4]  # Remove .lua extension
            script_path = os.path.join(script_dir, filename)

            try:
                sha = script_manager.load_script_from_file(
                    script_name,
                    script_path,
                    version="1.0.0"
                )
                loaded_scripts[script_name] = sha
                print(f"✓ Loaded {script_name}: {sha[:16]}...")
            except Exception as e:
                print(f"✗ Failed to load {script_name}: {e}")

    # Demonstrate script execution
    print("\n⚡ Executing Lua scripts...")

    # Rate limiter demo
    if 'rate_limiter' in loaded_scripts:
        print("\n🔥 Rate Limiter Demo:")
        user_key = "user:demo:123"

        for i in range(5):
            try:
                result = script_manager.execute_script(
                    'rate_limiter',
                    keys=[user_key],
                    args=[100, 3600, int(time.time()), 200]  # limit, window, time, burst
                )
                print(f"  Request {i+1}: {'ALLOWED' if result > 0 else 'BLOCKED'} (remaining: {result})")
            except Exception as e:
                print(f"  Request {i+1}: ERROR - {e}")

    # Smart cache demo
    if 'smart_cache' in loaded_scripts:
        print("\n🧠 Smart Cache Demo:")
        cache_key = "demo:cache:key"
        access_log = "demo:cache:access"
        stats_key = "demo:cache:stats"

        # Set cache entry
        result = script_manager.execute_script(
            'smart_cache',
            keys=[cache_key, access_log, stats_key],
            args=['SET', int(time.time()), 'cached_data', 3600, 1000]
        )
        print(f"  Cache SET result: {result}")

        # Get cache entry multiple times
        for i in range(3):
            result = script_manager.execute_script(
                'smart_cache',
                keys=[cache_key, access_log, stats_key],
                args=['GET', int(time.time())]
            )
            print(f"  Cache GET {i+1}: {result}")

        # Get cache statistics
        stats = script_manager.execute_script(
            'smart_cache',
            keys=[cache_key, access_log, stats_key],
            args=['STATS', int(time.time())]
        )
        print(f"  Cache stats: {stats}")

    # Job queue demo
    if 'job_queue' in loaded_scripts:
        print("\n⚙️  Job Queue Demo:")
        queue_key = "demo:queue"
        processing_key = "demo:processing"
        failed_key = "demo:failed"
        dead_key = "demo:dead"

        # Push jobs
        for i in range(3):
            job_id = f"job_{i+1}"
            result = script_manager.execute_script(
                'job_queue',
                keys=[queue_key, processing_key, failed_key, dead_key],
                args=['PUSH', int(time.time()), job_id, f'{{"task": "process_item_{i+1}"}}', 5, 3, 0]
            )
            print(f"  Pushed job {job_id}: {result}")

        # Pop and process jobs
        for i in range(3):
            jobs = script_manager.execute_script(
                'job_queue',
                keys=[queue_key, processing_key, failed_key, dead_key],
                args=['POP', int(time.time()), 1]
            )
            if jobs:
                job = jobs[0]
                print(f"  Popped job: {job[1]}")  # job[0] is job_id, job[1] is data

                # Mark as complete
                result = script_manager.execute_script(
                    'job_queue',
                    keys=[queue_key, processing_key, failed_key, dead_key],
                    args=['COMPLETE', int(time.time()), job[0]]
                )
                print(f"  Completed job: {result}")

        # Get queue statistics
        stats = script_manager.execute_script(
            'job_queue',
            keys=[queue_key, processing_key, failed_key, dead_key],
            args=['STATS', int(time.time())]
        )
        print(f"  Queue stats: {stats}")

    # Script management features
    print("\n📊 Script Management Features:")

    # List all loaded scripts
    scripts = script_manager.list_scripts()
    print(f"✓ Loaded {len(scripts)} scripts:")
    for name, info in scripts.items():
        cached = "✓" if info.get('cached') else "✗"
        print(f"  {name}: {info['sha'][:16]}... (cached: {cached})")

    # Get detailed script info
    if scripts:
        first_script = next(iter(scripts.keys()))
        info = script_manager.get_script_info(first_script)
        print(f"\n📋 Detailed info for '{first_script}':")
        for key, value in info.items():
            if key != 'metadata':  # Skip metadata for brevity
                print(f"  {key}: {value}")

    # Performance statistics
    stats = script_manager.get_script_stats()
    print(f"\n⚡ Script Manager Stats:")
    print(f"  Total scripts: {stats['total_scripts']}")
    print(f"  Cached scripts: {stats['cache_info']['cached_scripts']}")
    print(f"  Cache hit rate: {stats['cache_info']['cache_hit_rate']:.1%}")
    print(f"  Total source size: {stats['cache_info']['total_source_size']} bytes")

    # Demonstrate script validation
    print("\n🔍 Script Validation:")
    test_script = """
    local key = KEYS[1]
    local value = ARGV[1]
    redis.call('SET', key, value)
    return redis.call('GET', key)
    """
    validation = script_manager.validate_script(test_script)
    print(f"  Test script validation: {'✓ PASS' if validation['valid'] else '✗ FAIL'}")
    if validation['valid']:
        print(f"  SHA: {validation['sha'][:16]}...")
        print(f"  Cached: {validation['cached']}")

    print("\n✅ Lua Script Management Demo Complete!")
    print("\n🎯 Key Features Demonstrated:")
    print("  ✓ Optimized Cython script manager")
    print("  ✓ Automatic script loading and caching")
    print("  ✓ SHA-based script execution")
    print("  ✓ Comprehensive script metadata")
    print("  ✓ Real-world script execution examples")
    print("  ✓ Performance monitoring and statistics")
    print("  ✓ Script validation and debugging")

if __name__ == "__main__":
    demonstrate_lua_script_management()
