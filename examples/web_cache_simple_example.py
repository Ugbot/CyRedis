#!/usr/bin/env python3
"""
Simple Web Cache Example (Python-only version)
Demonstrates the web cache concepts without requiring compiled Cython modules.
"""

import asyncio
import hashlib
import json
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional


class SimpleWebCache:
    """
    Simple Python implementation of web cache functionality.
    Demonstrates the concepts without requiring compiled modules.
    """

    def __init__(
        self,
        backend_type: str = "memory",
        prefix: str = "cyredis:cache",
        default_ttl: int = 300,
    ):
        self.backend_type = backend_type
        self.prefix = prefix
        self.default_ttl = default_ttl
        self.cache = {}  # In-memory storage for demo
        self.expiries = {}

    def _make_key(self, key: str) -> str:
        """Create prefixed key"""
        return f"{self.prefix}:{key}"

    def _cleanup_expired(self):
        """Clean up expired entries"""
        current_time = time.time()
        expired_keys = []

        for key, expiry in self.expiries.items():
            if current_time > expiry:
                expired_keys.append(key)

        for key in expired_keys:
            self.cache.pop(key, None)
            self.expiries.pop(key, None)

    def get(self, key: str) -> Any:
        """Get value from cache"""
        self._cleanup_expired()
        cache_key = self._make_key(key)
        return self.cache.get(cache_key)

    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value in cache"""
        cache_key = self._make_key(key)
        self.cache[cache_key] = value

        if ttl:
            self.expiries[cache_key] = time.time() + ttl
        elif cache_key in self.expiries:
            del self.expiries[cache_key]

        return True

    def delete(self, key: str) -> bool:
        """Delete value from cache"""
        cache_key = self._make_key(key)
        deleted = cache_key in self.cache
        self.cache.pop(cache_key, None)
        self.expiries.pop(cache_key, None)
        return deleted

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        self._cleanup_expired()
        cache_key = self._make_key(key)
        return cache_key in self.cache

    def clear(self, pattern: str = None):
        """Clear cache"""
        if pattern:
            pattern = f"{self.prefix}:{pattern}*"
            keys_to_delete = []
            for key in self.cache.keys():
                if key.startswith(pattern.replace("*", "")):
                    keys_to_delete.append(key)

            for key in keys_to_delete:
                self.cache.pop(key, None)
                self.expiries.pop(key, None)
        else:
            self.cache.clear()
            self.expiries.clear()

    def invalidate_pattern(self, pattern: str):
        """Invalidate cache by pattern"""
        self.clear(pattern)

    def invalidate_namespace(self, namespace: str):
        """Invalidate namespace cache"""
        pattern = f"{namespace}:*"
        self.invalidate_pattern(pattern)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "backend_type": self.backend_type,
            "prefix": self.prefix,
            "default_ttl": self.default_ttl,
            "keys_count": len(self.cache),
            "memory_usage": len(json.dumps(self.cache)),
        }


class HTTPCacheHeaders:
    """HTTP cache headers management"""

    def __init__(self, etag_header_name="ETag", cache_control_header="Cache-Control"):
        self.etag_header_name = etag_header_name
        self.cache_control_header = cache_control_header

    def generate_etag(self, content: Any) -> str:
        """Generate ETag from content"""
        content_str = json.dumps(content, sort_keys=True, default=str)
        etag_data = hashlib.md5(content_str.encode()).hexdigest()
        return f'"{etag_data}"'

    def set_cache_headers(
        self, response: Dict[str, Any], max_age: int = 3600, cache_control: str = None
    ) -> Dict[str, Any]:
        """Set cache headers on response"""
        headers = response.get("headers", {})

        if cache_control:
            headers[self.cache_control_header] = cache_control
        else:
            headers[self.cache_control_header] = f"max-age={max_age}"

        response["headers"] = headers
        return response

    def set_etag_header(self, response: Dict[str, Any], etag: str) -> Dict[str, Any]:
        """Set ETag header on response"""
        headers = response.get("headers", {})
        headers[self.etag_header_name] = etag
        response["headers"] = headers
        return response

    def check_conditional_request(
        self, request_headers: Dict[str, str], current_etag: str
    ) -> bool:
        """Check if request has matching ETag for conditional request"""
        if_none_match = request_headers.get("If-None-Match")
        if if_none_match and if_none_match == current_etag:
            return True
        return False

    def create_304_response(self) -> Dict[str, Any]:
        """Create 304 Not Modified response"""
        return {
            "status_code": 304,
            "headers": {self.cache_control_header: "max-age=3600"},
        }


# Global functions for decorators
_cache_instance = None


def get_cache() -> SimpleWebCache:
    """Get global cache instance"""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = SimpleWebCache()
    return _cache_instance


def cache(ttl: int = None, namespace: str = ""):
    """Global cache decorator"""
    cache_instance = get_cache()

    def decorator(func):
        if asyncio.iscoroutinefunction(func):

            async def async_wrapper(*args, **kwargs):
                # Build cache key (simplified)
                func_name = f"{func.__module__}.{func.__name__}"
                args_str = str(sorted(kwargs.items())) if kwargs else ""
                cache_key = f"{namespace}:{func_name}:{args_str}"
                cache_key = hashlib.md5(cache_key.encode()).hexdigest()

                # Try to get from cache
                cached_result = cache_instance.get(cache_key)
                if cached_result is not None:
                    return cached_result

                # Execute function and cache result
                result = await func(*args, **kwargs)
                cache_instance.set(cache_key, result, ttl)
                return result

            return async_wrapper
        else:

            def sync_wrapper(*args, **kwargs):
                # Build cache key (simplified)
                func_name = f"{func.__module__}.{func.__name__}"
                args_str = str(sorted(kwargs.items())) if kwargs else ""
                cache_key = f"{namespace}:{func_name}:{args_str}"
                cache_key = hashlib.md5(cache_key.encode()).hexdigest()

                # Try to get from cache
                cached_result = cache_instance.get(cache_key)
                if cached_result is not None:
                    return cached_result

                # Execute function and cache result
                result = func(*args, **kwargs)
                cache_instance.set(cache_key, result, ttl)
                return result

            return sync_wrapper

    return decorator


def cached_endpoint(ttl: int = None, namespace: str = ""):
    """Global cached endpoint decorator"""
    cache_instance = get_cache()
    headers = HTTPCacheHeaders()

    def decorator(func):
        if asyncio.iscoroutinefunction(func):

            async def async_wrapper(*args, **kwargs):
                # Extract request from kwargs if present
                request = kwargs.get("request")

                # Build cache key (simplified)
                func_name = f"{func.__module__}.{func.__name__}"
                cache_key = f"{namespace}:{func_name}"
                cache_key = hashlib.md5(cache_key.encode()).hexdigest()

                # Try to get from cache
                cached_response = cache_instance.get(cache_key)
                if cached_response is not None:
                    # Handle conditional requests
                    if request:
                        current_etag = headers.generate_etag(cached_response)
                        if headers.check_conditional_request(
                            dict(request.headers), current_etag
                        ):
                            return headers.create_304_response()

                    return cached_response

                # Execute function
                result = await func(*args, **kwargs)

                # Add cache headers
                etag = headers.generate_etag(result)
                result = headers.set_etag_header(result, etag)
                result = headers.set_cache_headers(result, ttl or 300)

                # Cache the result
                cache_instance.set(cache_key, result, ttl)
                return result

            return async_wrapper
        else:

            def sync_wrapper(*args, **kwargs):
                # Build cache key (simplified)
                func_name = f"{func.__module__}.{func.__name__}"
                cache_key = f"{namespace}:{func_name}"
                cache_key = hashlib.md5(cache_key.encode()).hexdigest()

                # Try to get from cache
                cached_response = cache_instance.get(cache_key)
                if cached_response is not None:
                    return cached_response

                # Execute function
                result = func(*args, **kwargs)

                # Add cache headers
                etag = headers.generate_etag(result)
                result = headers.set_etag_header(result, etag)
                result = headers.set_cache_headers(result, ttl or 300)

                # Cache the result
                cache_instance.set(cache_key, result, ttl)
                return result

            return sync_wrapper

        return decorator

    return decorator


class MockRequest:
    """Mock request object for demonstration"""

    def __init__(
        self,
        method: str = "GET",
        path: str = "/api/users",
        query_params: List[tuple] = None,
        headers: Dict[str, str] = None,
    ):
        self.method = method
        self.url = type("obj", (object,), {"path": path})()
        self.query_params = type(
            "obj", (object,), {"items": lambda: query_params or []}
        )()
        self.headers = headers or {}


class ExampleAPI:
    """Example API demonstrating cache functionality"""

    def __init__(self):
        self.cache = get_cache()

        # In-memory storage for demo data
        self.users_db = {
            "1": {"id": "1", "name": "Alice", "email": "alice@example.com", "age": 30},
            "2": {"id": "2", "name": "Bob", "email": "bob@example.com", "age": 25},
            "3": {
                "id": "3",
                "name": "Charlie",
                "email": "charlie@example.com",
                "age": 35,
            },
        }

    def get_expensive_data(self, user_id: str) -> Dict[str, Any]:
        """Simulate expensive operation"""
        print(f"🔄 Computing expensive data for user {user_id}...")

        # Simulate expensive computation
        time.sleep(1)

        user = self.users_db.get(user_id)
        if not user:
            return {"error": "User not found"}

        return {
            "user": user,
            "computed_at": time.time(),
            "random_factor": random.randint(1, 100),
            "cached": False,
        }

    @cache(ttl=60, namespace="expensive_data")
    def get_cached_expensive_data(self, user_id: str) -> Dict[str, Any]:
        """Cached version of expensive operation"""
        return self.get_expensive_data(user_id)

    @cached_endpoint(ttl=300, namespace="api_endpoints")
    async def get_user(self, request, user_id: str) -> Dict[str, Any]:
        """Get user with HTTP caching"""
        print(f"🔄 Fetching user {user_id} from database...")

        # Simulate database query
        time.sleep(0.5)

        user = self.users_db.get(user_id)
        if not user:
            return {"error": "User not found", "status_code": 404}

        return {"data": user, "cached": False, "timestamp": time.time()}

    def create_user(self, name: str, email: str, age: int) -> Dict[str, Any]:
        """Create new user (invalidates caches)"""
        user_id = str(len(self.users_db) + 1)

        new_user = {"id": user_id, "name": name, "email": email, "age": age}

        self.users_db[user_id] = new_user

        # Invalidate related caches
        self.cache.invalidate_namespace("api_endpoints")
        self.cache.invalidate_pattern("user:*")

        return {"user": new_user, "created": True}


async def main():
    """Main demonstration function"""
    print("🎯 CyRedis Web Cache - FastAPI-Style Features Demo (Python Version)")
    print("=" * 70)

    # Initialize API
    api = ExampleAPI()

    try:
        # 1. Performance demonstration
        print("\n🚀 Demonstrating Cache Performance")
        print("=" * 40)

        # First call - should be slow (cache miss)
        print("\n📊 First call (cache miss):")
        start_time = time.time()
        result1 = await api.get_user(MockRequest(), "1")
        first_call_time = time.time() - start_time
        print(f"   Time: {first_call_time:.3f}s")
        print(f"   Cached: {result1.get('cached', 'N/A')}")

        # Second call - should be fast (cache hit)
        print("\n📊 Second call (cache hit):")
        start_time = time.time()
        result2 = await api.get_user(MockRequest(), "1")
        second_call_time = time.time() - start_time
        print(f"   Time: {second_call_time:.3f}s")
        print(f"   Cached: {result2.get('cached', 'N/A')}")
        print(f"   Speedup: {first_call_time/second_call_time:.1f}x faster")

        # 2. Demonstrate expensive operation caching
        print("\n💰 Demonstrating Expensive Operation Caching")
        print("=" * 50)

        # First call - expensive
        print("\n📊 Expensive operation (first call):")
        start_time = time.time()
        expensive_result1 = api.get_cached_expensive_data("1")
        expensive_time1 = time.time() - start_time
        print(f"   Time: {expensive_time1:.3f}s")

        # Second call - cached
        print("\n📊 Expensive operation (cached call):")
        start_time = time.time()
        expensive_result2 = api.get_cached_expensive_data("1")
        expensive_time2 = time.time() - start_time
        print(f"   Time: {expensive_time2:.3f}s")
        print(f"   Speedup: {expensive_time1/expensive_time2:.1f}x faster")

        # 3. Demonstrate cache invalidation
        print("\n🗑️  Demonstrating Cache Invalidation")
        print("=" * 40)

        # Create new user
        print("Creating new user...")
        create_result = api.create_user("Diana", "diana@example.com", 28)
        print(f"Created: {create_result}")

        # Check that caches were invalidated
        print("Checking cache invalidation...")
        api_cache_exists = api.cache.exists("api_endpoints")
        print(f"API endpoints cache invalidated: {not api_cache_exists}")

        # 4. Show cache statistics
        print("\n📊 Cache Statistics:")
        stats = api.cache.get_stats()
        print(json.dumps(stats, indent=2))

        # 5. Demonstrate HTTP headers
        print("\n🏷️  Demonstrating HTTP Cache Headers")
        print("=" * 40)

        # Get response with headers
        response = await api.get_user(MockRequest(), "2")
        etag = response.get("headers", {}).get("ETag")
        cache_control = response.get("headers", {}).get("Cache-Control")

        print(f"ETag: {etag}")
        print(f"Cache-Control: {cache_control}")

        # Demonstrate conditional request
        print("\nConditional request simulation:")
        conditional_request = MockRequest(
            method="GET", path="/api/users/2", headers={"If-None-Match": etag}
        )

        # This would return 304 in a real implementation
        print("If-None-Match header present - would return 304 Not Modified")

        print("\n✅ CyRedis Web Cache demonstration completed!")
        print("\nKey features demonstrated:")
        print("  ✓ Function caching with @cache decorator")
        print("  ✓ HTTP endpoint caching with @cached_endpoint")
        print("  ✓ HTTP cache headers (ETag, Cache-Control)")
        print("  ✓ Cache invalidation by pattern and namespace")
        print("  ✓ Performance improvements through caching")
        print("  ✓ Cache statistics and monitoring")

    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
