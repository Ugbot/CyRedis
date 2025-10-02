#!/usr/bin/env python3
"""
Example of Atomic Lua Script Deployment with Testing and Function Mapping
Demonstrates the new atomic_load_and_test and atomic_deploy_scripts capabilities
"""

import time
import json
from optimized_redis import OptimizedRedis
from optimized_lua_script_manager import OptimizedLuaScriptManager

def demonstrate_atomic_script_loading():
    """Demonstrate atomic script loading with testing and function mapping."""
    print("🚀 CyRedis Atomic Lua Script Deployment Demo")
    print("=" * 60)

    # Initialize Redis client and script manager
    redis = OptimizedRedis()
    script_manager = OptimizedLuaScriptManager(redis, namespace="demo:atomic")

    # Example 1: Atomic loading with testing for a simple counter script
    print("\n📜 Example 1: Atomic Counter Script Deployment")
    print("-" * 50)

    counter_script = """
    local key = KEYS[1]
    local operation = ARGV[1]

    if operation == "INCR" then
        return redis.call('INCR', key)
    elseif operation == "DECR" then
        return redis.call('DECR', key)
    elseif operation == "GET" then
        return redis.call('GET', key) or 0
    elseif operation == "RESET" then
        return redis.call('SET', key, 0)
    end

    return redis.call('GET', key) or 0
    """

    counter_tests = {
        "test_get_initial": {
            "keys": ["test_counter"],
            "args": ["GET"],
            "expected": 0
        },
        "test_increment": {
            "keys": ["test_counter"],
            "args": ["INCR"],
            "expected": 1
        },
        "test_decrement": {
            "keys": ["test_counter"],
            "args": ["DECR"],
            "expected": 0
        },
        "test_reset": {
            "keys": ["test_counter"],
            "args": ["RESET"],
            "expected": "OK"
        }
    }

    print("🔄 Atomically loading and testing counter script...")
    result = script_manager.atomic_load_and_test(
        name="counter",
        script=counter_script,
        version="1.0.0",
        test_cases=counter_tests,
        metadata={"author": "cyredis", "purpose": "simple_counter"}
    )

    if result['success']:
        print("✅ Counter script deployed successfully!")
        print(f"   SHA: {result['sha'][:16]}...")
        print(f"   Tests passed: {len(result['tests_passed'])}")
        print(f"   Function mappings: {list(result['functions'].keys())}")

        # Use the mapped functions
        counter = result['functions']
        print(f"\n🎯 Using mapped functions:")
        print(f"   Current value: {counter['execute'](['test_counter'], ['GET'])}")
        print(f"   After increment: {counter['execute'](['test_counter'], ['INCR'])}")
        print(f"   After increment: {counter['execute'](['test_counter'], ['INCR'])}")
        print(f"   After decrement: {counter['execute'](['test_counter'], ['DECR'])}")

    else:
        print("❌ Counter script deployment failed!")
        print(f"   Error: {result['error']}")
        if result['tests_failed']:
            print(f"   Failed tests: {len(result['tests_failed'])}")
            for failure in result['tests_failed'][:3]:  # Show first 3 failures
                print(f"     - {failure['test']}: {failure.get('error', 'unexpected result')}")

    # Example 2: Atomic deployment of multiple scripts
    print("\n📜 Example 2: Atomic Multi-Script Deployment")
    print("-" * 50)

    # Define multiple scripts with their tests
    scripts_config = {
        "string_utils": {
            "script": """
            local operation = ARGV[1]
            local key = KEYS[1]

            if operation == "UPPER" then
                local value = redis.call('GET', key) or ""
                return string.upper(value)
            elseif operation == "LOWER" then
                local value = redis.call('GET', key) or ""
                return string.lower(value)
            elseif operation == "LEN" then
                local value = redis.call('GET', key) or ""
                return string.len(value)
            end

            return redis.call('GET', key) or ""
            """,
            "version": "1.0.0",
            "test_cases": {
                "test_upper": {
                    "keys": ["test_string"],
                    "args": ["UPPER"],
                    "expected": ""  # Empty string initially
                },
                "test_len": {
                    "keys": ["test_string"],
                    "args": ["LEN"],
                    "expected": 0
                }
            },
            "metadata": {"type": "string_utils", "complexity": "simple"}
        },

        "math_ops": {
            "script": """
            local key = KEYS[1]
            local operation = ARGV[1]
            local value = tonumber(ARGV[2] or 0)

            if operation == "ADD" then
                return redis.call('INCRBYFLOAT', key, value)
            elseif operation == "SUB" then
                return redis.call('INCRBYFLOAT', key, -value)
            elseif operation == "MUL" then
                local current = tonumber(redis.call('GET', key) or 0)
                local result = current * value
                redis.call('SET', key, result)
                return result
            elseif operation == "DIV" then
                local current = tonumber(redis.call('GET', key) or 0)
                if value == 0 then return "DIVISION_BY_ZERO" end
                local result = current / value
                redis.call('SET', key, result)
                return result
            end

            return redis.call('GET', key) or 0
            """,
            "version": "1.0.0",
            "test_cases": {
                "test_add": {
                    "keys": ["test_math"],
                    "args": ["ADD", "5"],
                    "expected": 5.0
                },
                "test_subtract": {
                    "keys": ["test_math"],
                    "args": ["SUB", "3"],
                    "expected": 2.0
                },
                "test_multiply": {
                    "keys": ["test_math"],
                    "args": ["MUL", "4"],
                    "expected": 8.0
                }
            },
            "metadata": {"type": "math_operations", "precision": "float"}
        }
    }

    print("🔄 Atomically deploying multiple scripts...")
    deployment_result = script_manager.atomic_deploy_scripts(scripts_config)

    if deployment_result['success']:
        print("✅ All scripts deployed successfully!")
        print(f"   Deployed scripts: {deployment_result['deployed_scripts']}")

        # Demonstrate using the deployed scripts
        print(f"\n🎯 Using deployed script functions:")

        # String utils
        redis.set("demo_string", "Hello World")
        string_utils = deployment_result['function_mappings']['string_utils']
        upper_result = string_utils['execute'](["demo_string"], ["UPPER"])
        print(f"   String uppercase: '{upper_result}'")

        # Math operations
        math_ops = deployment_result['function_mappings']['math_ops']
        add_result = math_ops['execute'](["demo_math"], ["ADD", "10"])
        mul_result = math_ops['execute'](["demo_math"], ["MUL", "2"])
        print(f"   Math operations: ADD(10) = {add_result}, MUL(2) = {mul_result}")

    else:
        print("❌ Multi-script deployment failed!")
        print(f"   Error: {deployment_result.get('error', 'Unknown error')}")
        print(f"   Failed scripts: {len(deployment_result['failed_scripts'])}")

        for failure in deployment_result['failed_scripts']:
            print(f"     - {failure['name']}: {failure['error']}")

    # Example 3: Demonstrating rollback on failure
    print("\n📜 Example 3: Rollback on Deployment Failure")
    print("-" * 50)

    # Create a script that will fail testing
    failing_scripts = {
        "good_script": {
            "script": "return redis.call('PING')",
            "version": "1.0.0",
            "test_cases": {
                "ping_test": {
                    "keys": [],
                    "args": [],
                    "expected": "PONG"
                }
            }
        },
        "bad_script": {
            "script": "return redis.call('INVALID_COMMAND')",  # This will fail
            "version": "1.0.0",
            "test_cases": {
                "invalid_test": {
                    "keys": [],
                    "args": [],
                    "expected": "SHOULD_FAIL"
                }
            }
        }
    }

    print("🔄 Attempting deployment that will fail...")
    rollback_result = script_manager.atomic_deploy_scripts(failing_scripts)

    if not rollback_result['success']:
        print("✅ Rollback worked correctly!")
        print(f"   Deployment failed as expected: {rollback_result.get('error', 'Unknown error')}")
        print(f"   Scripts that were deployed before failure: {rollback_result['deployed_scripts']}")
        print(f"   Scripts that failed: {len(rollback_result['failed_scripts'])}")

        # Verify rollback - good_script should not be registered
        scripts = script_manager.list_scripts()
        if 'good_script' not in scripts:
            print("✅ Rollback confirmed - good_script was not registered")
        else:
            print("❌ Rollback failed - good_script was still registered")

    # Final statistics
    print("\n📊 Final Deployment Statistics")
    print("-" * 50)

    stats = script_manager.get_script_stats()
    scripts_info = script_manager.list_scripts()

    print(f"Total scripts loaded: {stats['total_scripts']}")
    print(f"Scripts in Redis cache: {stats['cache_info']['cached_scripts']}")
    print(f"Cache hit rate: {stats['cache_info']['cache_hit_rate']:.1%}")
    print(f"Total script source size: {stats['cache_info']['total_source_size']} bytes")

    print(f"\n📋 Registered Scripts:")
    for name, info in scripts_info.items():
        print(f"  {name}: v{info.get('version', 'unknown')} ({info['sha'][:16]}...)")

    print("\n✅ Atomic Script Deployment Demo Complete!")
    print("\n🎯 Key Features Demonstrated:")
    print("  ✓ Atomic script loading with validation")
    print("  ✓ Automatic test execution during deployment")
    print("  ✓ Function mapping for easy script usage")
    print("  ✓ Multi-script atomic deployment")
    print("  ✓ Automatic rollback on failure")
    print("  ✓ Comprehensive deployment statistics")
    print("  ✓ Production-ready error handling")

if __name__ == "__main__":
    demonstrate_atomic_script_loading()
