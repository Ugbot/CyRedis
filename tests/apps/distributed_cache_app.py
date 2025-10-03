#!/usr/bin/env python3
"""
Distributed Caching Application

A distributed cache implementation demonstrating CyRedis caching patterns:
- Read-through and write-through caching
- Cache invalidation strategies
- TTL-based expiration
- Cache warming
- Cache statistics

Features:
- Multiple cache layers (L1/L2)
- Automatic cache warming
- Cache hit/miss tracking
- Pattern-based invalidation
- Namespace support
- JSON serialization

Usage:
    # Interactive mode
    python distributed_cache_app.py

    # Demo mode
    python distributed_cache_app.py --demo

    # Warm cache from data
    python distributed_cache_app.py --warm data.json
"""

import sys
import argparse
import json
import time
from typing import Any, Optional, Callable, Dict, List
from datetime import datetime
from functools import wraps

try:
    from optimized_redis import OptimizedRedis
except ImportError:
    print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
    sys.exit(1)


class DistributedCache:
    """Distributed cache with read-through and write-through support"""

    def __init__(self, redis: OptimizedRedis, namespace: str = "cache", default_ttl: int = 300):
        """Initialize distributed cache

        Args:
            redis: Redis client instance
            namespace: Cache namespace
            default_ttl: Default TTL in seconds
        """
        self.redis = redis
        self.namespace = namespace
        self.default_ttl = default_ttl
        self.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}

    def _key(self, key: str) -> str:
        """Generate namespaced cache key"""
        return f"{self.namespace}:{key}"

    def get(self, key: str, loader: Optional[Callable] = None, ttl: Optional[int] = None) -> Optional[Any]:
        """Get value from cache with optional read-through

        Args:
            key: Cache key
            loader: Optional function to load data on cache miss
            ttl: Optional TTL override

        Returns:
            Cached value or loaded value
        """
        cache_key = self._key(key)

        # Try to get from cache
        value = self.redis.get(cache_key)

        if value is not None:
            self.stats["hits"] += 1
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value

        # Cache miss
        self.stats["misses"] += 1

        # Load data if loader provided
        if loader:
            value = loader()
            if value is not None:
                self.set(key, value, ttl)
            return value

        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache

        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional TTL override

        Returns:
            True if successful
        """
        cache_key = self._key(key)
        ttl = ttl or self.default_ttl

        try:
            # Serialize value
            if not isinstance(value, str):
                value = json.dumps(value)

            # Set with TTL
            self.redis.setex(cache_key, ttl, value)
            self.stats["sets"] += 1
            return True

        except Exception as e:
            print(f"Error setting cache key {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """Delete key from cache

        Args:
            key: Cache key

        Returns:
            True if key was deleted
        """
        cache_key = self._key(key)
        result = self.redis.delete(cache_key)
        if result > 0:
            self.stats["deletes"] += 1
            return True
        return False

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern

        Args:
            pattern: Key pattern (supports wildcards)

        Returns:
            Number of keys deleted
        """
        cache_pattern = self._key(pattern)
        keys = self.redis.keys(cache_pattern)

        if keys:
            count = self.redis.delete(*keys)
            self.stats["deletes"] += count
            return count

        return 0

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics

        Returns:
            Dictionary with cache stats
        """
        total_requests = self.stats["hits"] + self.stats["misses"]
        hit_rate = (self.stats["hits"] / total_requests * 100) if total_requests > 0 else 0

        return {
            **self.stats,
            "total_requests": total_requests,
            "hit_rate": f"{hit_rate:.2f}%"
        }

    def reset_stats(self):
        """Reset statistics counters"""
        self.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}

    def warm(self, data: Dict[str, Any], ttl: Optional[int] = None):
        """Warm cache with data

        Args:
            data: Dictionary of key-value pairs to cache
            ttl: Optional TTL for all keys
        """
        count = 0
        for key, value in data.items():
            if self.set(key, value, ttl):
                count += 1
        return count


def cached(cache: DistributedCache, key_pattern: str, ttl: Optional[int] = None):
    """Decorator for caching function results

    Args:
        cache: Cache instance
        key_pattern: Cache key pattern (can use {arg_name})
        ttl: Optional TTL override
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from pattern
            cache_key = key_pattern
            if "{" in cache_key:
                # Simple template substitution
                import inspect
                sig = inspect.signature(func)
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                for param_name, param_value in bound.arguments.items():
                    cache_key = cache_key.replace(f"{{{param_name}}}", str(param_value))

            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result

            # Call function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result

        return wrapper
    return decorator


class CacheApplication:
    """Distributed cache application"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        """Initialize cache application

        Args:
            host: Redis host
            port: Redis port
        """
        self.redis = OptimizedRedis(host=host, port=port)
        self.cache = DistributedCache(self.redis, namespace="app", default_ttl=300)
        print(f"✓ Connected to Redis at {host}:{port}")

    def demo_basic_caching(self):
        """Demonstrate basic caching operations"""
        print("\n" + "="*60)
        print("1. Basic Caching Operations")
        print("="*60)

        # Set some values
        print("\nSetting cache values...")
        self.cache.set("user:1", {"id": 1, "name": "Alice", "email": "alice@example.com"}, ttl=60)
        self.cache.set("user:2", {"id": 2, "name": "Bob", "email": "bob@example.com"}, ttl=60)
        self.cache.set("product:100", {"id": 100, "name": "Widget", "price": 29.99}, ttl=120)
        print("✓ Cached 3 items")

        # Get values
        print("\nRetrieving from cache...")
        user1 = self.cache.get("user:1")
        print(f"✓ user:1 = {user1}")

        product = self.cache.get("product:100")
        print(f"✓ product:100 = {product}")

        # Cache miss
        missing = self.cache.get("user:999")
        print(f"✗ user:999 = {missing} (cache miss)")

    def demo_read_through(self):
        """Demonstrate read-through caching"""
        print("\n" + "="*60)
        print("2. Read-Through Caching")
        print("="*60)

        # Simulate database
        database = {
            "order:1": {"id": 1, "customer": "Alice", "total": 99.99},
            "order:2": {"id": 2, "customer": "Bob", "total": 149.50},
        }

        def load_order(order_id: int):
            """Simulate loading from database"""
            print(f"  📀 Loading order {order_id} from database...")
            time.sleep(0.1)  # Simulate DB latency
            return database.get(f"order:{order_id}")

        # First access - cache miss, loads from DB
        print("\nFirst access (cache miss):")
        order = self.cache.get("order:1", loader=lambda: load_order(1), ttl=60)
        print(f"✓ Got order: {order}")

        # Second access - cache hit
        print("\nSecond access (cache hit):")
        order = self.cache.get("order:1", loader=lambda: load_order(1), ttl=60)
        print(f"✓ Got order: {order} (from cache)")

    def demo_invalidation(self):
        """Demonstrate cache invalidation"""
        print("\n" + "="*60)
        print("3. Cache Invalidation")
        print("="*60)

        # Cache some session data
        print("\nCaching session data...")
        self.cache.set("session:abc123", {"user_id": 1, "login_time": time.time()})
        self.cache.set("session:def456", {"user_id": 2, "login_time": time.time()})
        self.cache.set("session:ghi789", {"user_id": 3, "login_time": time.time()})
        print("✓ Cached 3 sessions")

        # Invalidate single key
        print("\nInvalidating single session...")
        self.cache.delete("session:abc123")
        print("✓ Deleted session:abc123")

        # Invalidate by pattern
        print("\nInvalidating all sessions...")
        count = self.cache.invalidate_pattern("session:*")
        print(f"✓ Deleted {count} session(s)")

    def demo_decorator(self):
        """Demonstrate caching decorator"""
        print("\n" + "="*60)
        print("4. Caching Decorator")
        print("="*60)

        @cached(self.cache, "fib:{n}", ttl=120)
        def fibonacci(n: int) -> int:
            """Calculate Fibonacci number (cached)"""
            print(f"  🔢 Computing fibonacci({n})...")
            if n <= 1:
                return n
            return fibonacci(n-1) + fibonacci(n-2)

        # First call - computes
        print("\nFirst call (computes):")
        result = fibonacci(10)
        print(f"✓ fibonacci(10) = {result}")

        # Second call - from cache
        print("\nSecond call (cached):")
        result = fibonacci(10)
        print(f"✓ fibonacci(10) = {result} (from cache)")

    def demo_cache_warming(self):
        """Demonstrate cache warming"""
        print("\n" + "="*60)
        print("5. Cache Warming")
        print("="*60)

        # Prepare data to warm cache
        warm_data = {
            "config:app_name": "MyApp",
            "config:version": "1.0.0",
            "config:max_users": 1000,
            "config:features": ["auth", "api", "admin"]
        }

        print("\nWarming cache with configuration data...")
        count = self.cache.warm(warm_data, ttl=3600)
        print(f"✓ Warmed {count} configuration keys")

        # Verify
        print("\nVerifying cached data...")
        app_name = self.cache.get("config:app_name")
        print(f"✓ config:app_name = {app_name}")

    def show_statistics(self):
        """Show cache statistics"""
        print("\n" + "="*60)
        print("Cache Statistics")
        print("="*60)

        stats = self.cache.get_stats()
        print(f"Total Requests: {stats['total_requests']}")
        print(f"Cache Hits:     {stats['hits']}")
        print(f"Cache Misses:   {stats['misses']}")
        print(f"Hit Rate:       {stats['hit_rate']}")
        print(f"Sets:           {stats['sets']}")
        print(f"Deletes:        {stats['deletes']}")

    def run_demo(self):
        """Run complete demonstration"""
        print("\n" + "="*60)
        print("Distributed Cache Application - Demo Mode")
        print("="*60)

        self.demo_basic_caching()
        self.demo_read_through()
        self.demo_invalidation()
        self.demo_decorator()
        self.demo_cache_warming()
        self.show_statistics()

        print("\n✓ Demo complete!")

    def interactive_mode(self):
        """Run interactive cache explorer"""
        print("\n" + "="*60)
        print("Distributed Cache - Interactive Mode")
        print("="*60)
        print("Commands: get, set, delete, invalidate, stats, warm, demo, quit")
        print()

        while True:
            try:
                command = input("cache> ").strip()
                if not command:
                    continue

                parts = command.split(None, 2)
                cmd = parts[0].lower()

                if cmd in ("quit", "exit"):
                    break

                elif cmd == "get":
                    if len(parts) < 2:
                        print("Usage: get <key>")
                    else:
                        value = self.cache.get(parts[1])
                        print(f"✓ {parts[1]} = {value}")

                elif cmd == "set":
                    if len(parts) < 3:
                        print("Usage: set <key> <value>")
                    else:
                        self.cache.set(parts[1], parts[2])
                        print(f"✓ Set {parts[1]}")

                elif cmd == "delete":
                    if len(parts) < 2:
                        print("Usage: delete <key>")
                    else:
                        self.cache.delete(parts[1])
                        print(f"✓ Deleted {parts[1]}")

                elif cmd == "invalidate":
                    if len(parts) < 2:
                        print("Usage: invalidate <pattern>")
                    else:
                        count = self.cache.invalidate_pattern(parts[1])
                        print(f"✓ Invalidated {count} key(s)")

                elif cmd == "stats":
                    self.show_statistics()

                elif cmd == "demo":
                    self.run_demo()

                else:
                    print(f"Unknown command: {cmd}")

            except KeyboardInterrupt:
                print("\nUse 'quit' to exit")
            except Exception as e:
                print(f"Error: {e}")

    def close(self):
        """Close Redis connection"""
        self.redis.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Distributed Caching Application",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--demo", action="store_true", help="Run demo mode")
    parser.add_argument("--warm", help="Warm cache from JSON file")

    args = parser.parse_args()

    try:
        app = CacheApplication(host=args.host, port=args.port)

        if args.warm:
            with open(args.warm, 'r') as f:
                data = json.load(f)
            count = app.cache.warm(data)
            print(f"✓ Warmed cache with {count} items from {args.warm}")

        elif args.demo:
            app.run_demo()
        else:
            app.interactive_mode()

        app.close()

    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
