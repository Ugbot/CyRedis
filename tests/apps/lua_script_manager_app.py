#!/usr/bin/env python3
"""
Lua Script Manager Application

Demonstrates:
- Loading Lua scripts from files
- Script caching with EVALSHA
- Managing multiple scripts
- Atomic operations with Lua
- Script versioning

Usage:
    # Load all scripts
    python lua_script_manager_app.py load

    # List loaded scripts
    python lua_script_manager_app.py list

    # Execute a script
    python lua_script_manager_app.py exec rate_limiter key 10 60

    # Test rate limiter
    python lua_script_manager_app.py test-ratelimit --requests 20

    # Benchmark script execution
    python lua_script_manager_app.py benchmark
"""

import argparse
import sys
import os
import time
import hashlib
from typing import Dict, Any, Optional

try:
    from optimized_redis import OptimizedRedis as Redis
except ImportError:
    try:
        from redis_wrapper import HighPerformanceRedis as Redis
    except ImportError:
        print("Error: Could not import CyRedis client")
        print("Please build CyRedis first: bash build_optimized.sh")
        sys.exit(1)


class LuaScriptManager:
    """Manage Lua scripts with caching"""

    def __init__(self, host='localhost', port=6379, scripts_dir='lua_scripts'):
        self.redis = Redis(host=host, port=port)
        self.scripts_dir = scripts_dir
        self.scripts = {}  # name -> (script_content, sha)
        self.metadata_key = "lua:scripts:metadata"

    def load_script_file(self, filename: str) -> tuple:
        """Load a Lua script from file"""
        filepath = os.path.join(self.scripts_dir, filename)

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Script not found: {filepath}")

        with open(filepath, 'r') as f:
            script_content = f.read()

        # Calculate SHA locally for verification
        local_sha = hashlib.sha1(script_content.encode('utf-8')).hexdigest()

        return script_content, local_sha

    def load_script(self, name: str, filename: str) -> str:
        """Load a script into Redis and cache it"""
        script_content, local_sha = self.load_script_file(filename)

        # Load into Redis
        try:
            sha = self.redis.script_load(script_content)

            if isinstance(sha, bytes):
                sha = sha.decode('utf-8')

            # Store in local cache
            self.scripts[name] = (script_content, sha)

            # Store metadata in Redis
            self.redis.hset(
                self.metadata_key,
                name,
                f"{filename}:{sha}"
            )

            print(f"✓ Loaded {name} from {filename}")
            print(f"  SHA: {sha}")

            return sha

        except Exception as e:
            print(f"✗ Failed to load {name}: {e}")
            raise

    def load_all_scripts(self):
        """Load all Lua scripts from directory"""
        if not os.path.exists(self.scripts_dir):
            print(f"Scripts directory not found: {self.scripts_dir}")
            return

        lua_files = [f for f in os.listdir(self.scripts_dir) if f.endswith('.lua')]

        print(f"Loading {len(lua_files)} Lua scripts...\n")

        for lua_file in lua_files:
            name = lua_file.replace('.lua', '')
            try:
                self.load_script(name, lua_file)
            except Exception as e:
                print(f"  Warning: Could not load {lua_file}: {e}")

        print(f"\n✓ Loaded {len(self.scripts)} scripts")

    def execute_script(self, name: str, keys: list = None, args: list = None):
        """Execute a loaded script using EVALSHA"""
        if name not in self.scripts:
            raise ValueError(f"Script not loaded: {name}")

        script_content, sha = self.scripts[name]
        keys = keys or []
        args = args or []

        try:
            result = self.redis.evalsha(sha, len(keys), *keys, *args)
            return result
        except Exception as e:
            # If script not found in Redis cache, reload it
            if 'NOSCRIPT' in str(e):
                print(f"  Script not in cache, reloading...")
                sha = self.redis.script_load(script_content)
                self.scripts[name] = (script_content, sha)
                result = self.redis.evalsha(sha, len(keys), *keys, *args)
                return result
            raise

    def list_scripts(self):
        """List all loaded scripts"""
        if not self.scripts:
            print("No scripts loaded")
            return

        print(f"\n{'Name':<20} {'SHA':<45} {'Status'}")
        print("-" * 75)

        for name, (content, sha) in self.scripts.items():
            # Check if script exists in Redis
            try:
                exists = self.redis.script_exists(sha)
                if isinstance(exists, list):
                    status = "✓ Cached" if exists[0] else "✗ Not cached"
                else:
                    status = "✓ Cached" if exists else "✗ Not cached"
            except:
                status = "? Unknown"

            print(f"{name:<20} {sha:<45} {status}")

    def flush_cache(self):
        """Flush all scripts from Redis cache"""
        try:
            self.redis.script_flush()
            print("✓ Flushed script cache")
        except Exception as e:
            print(f"✗ Failed to flush cache: {e}")

    def test_rate_limiter(self, requests: int = 10):
        """Test the rate limiter script"""
        if 'rate_limiter' not in self.scripts:
            print("Loading rate limiter script...")
            self.load_script('rate_limiter', 'rate_limiter.lua')

        key = 'test:rate_limit:demo'
        limit = 5
        window = 10

        print(f"\nTesting rate limiter (limit: {limit} requests per {window}s)")
        print(f"Attempting {requests} requests...\n")

        allowed = 0
        denied = 0

        for i in range(requests):
            try:
                result = self.execute_script(
                    'rate_limiter',
                    keys=[key],
                    args=[str(limit), str(window), str(int(time.time()))]
                )

                if result == 1:
                    print(f"  Request {i+1}: ✓ Allowed")
                    allowed += 1
                else:
                    print(f"  Request {i+1}: ✗ Denied (rate limit exceeded)")
                    denied += 1

            except Exception as e:
                print(f"  Request {i+1}: Error - {e}")

            time.sleep(0.1)

        # Cleanup
        self.redis.delete(key)

        print(f"\nResults:")
        print(f"  Allowed: {allowed}")
        print(f"  Denied:  {denied}")
        print(f"  Rate limit working: {'✓' if denied > 0 else '✗'}")

    def benchmark_evalsha(self, iterations: int = 1000):
        """Benchmark EVALSHA performance"""
        # Simple script that returns a value
        simple_script = "return ARGV[1]"
        sha = self.redis.script_load(simple_script)

        print(f"\nBenchmarking EVALSHA ({iterations} iterations)...")

        start = time.time()
        for i in range(iterations):
            self.redis.evalsha(sha, 0, str(i))
        elapsed = time.time() - start

        ops_per_sec = iterations / elapsed
        latency_ms = (elapsed / iterations) * 1000

        print(f"\nResults:")
        print(f"  Total time:       {elapsed:.3f}s")
        print(f"  Operations/sec:   {ops_per_sec:.2f}")
        print(f"  Avg latency:      {latency_ms:.3f}ms")


def main():
    parser = argparse.ArgumentParser(description="Lua Script Manager")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Load command
    load_parser = subparsers.add_parser("load", help="Load Lua scripts")
    load_parser.add_argument("--script", help="Load specific script")

    # List command
    subparsers.add_parser("list", help="List loaded scripts")

    # Execute command
    exec_parser = subparsers.add_parser("exec", help="Execute a script")
    exec_parser.add_argument("name", help="Script name")
    exec_parser.add_argument("keys", nargs="*", help="Keys for the script")

    # Flush command
    subparsers.add_parser("flush", help="Flush script cache")

    # Test rate limiter
    test_rl_parser = subparsers.add_parser("test-ratelimit", help="Test rate limiter")
    test_rl_parser.add_argument("--requests", type=int, default=10, help="Number of requests")

    # Benchmark
    bench_parser = subparsers.add_parser("benchmark", help="Benchmark EVALSHA")
    bench_parser.add_argument("--iterations", type=int, default=1000, help="Number of iterations")

    args = parser.parse_args()

    manager = LuaScriptManager()

    if args.command == "load":
        if args.script:
            manager.load_script(args.script, f"{args.script}.lua")
        else:
            manager.load_all_scripts()

    elif args.command == "list":
        manager.load_all_scripts()
        manager.list_scripts()

    elif args.command == "exec":
        manager.load_all_scripts()
        result = manager.execute_script(args.name, keys=args.keys)
        print(f"\nResult: {result}")

    elif args.command == "flush":
        manager.flush_cache()

    elif args.command == "test-ratelimit":
        manager.test_rate_limiter(requests=args.requests)

    elif args.command == "benchmark":
        manager.benchmark_evalsha(iterations=args.iterations)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
