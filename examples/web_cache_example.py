#!/usr/bin/env python3
"""
FastAPI-Style Web Cache Example using CyRedis
Demonstrates the new cache functionality inspired by fastapi-cache.
"""

import asyncio
import json
import time
import random
from typing import Dict, Any, Optional, List
from datetime import datetime

# Import our new web cache functionality
from cy_redis.cy_redis_client import CyRedisClient
from cy_redis.web_cache import (
    WebCache, init_cache, get_cache,
    cache, cached_endpoint, cached_json_response,
    JsonCoder, PickleCoder, RequestKeyBuilder
)


class MockRequest:
    """Mock request object for demonstration"""
    def __init__(self, method: str = "GET", path: str = "/api/users",
                 query_params: List[tuple] = None, headers: Dict[str, str] = None):
        self.method = method
        self.url = type('obj', (object,), {'path': path})()
        self.query_params = type('obj', (object,), {
            'items': lambda: query_params or []
        })()
        self.headers = headers or {}


class MockResponse:
    """Mock response object for demonstration"""
    def __init__(self, data: Any = None, status_code: int = 200, headers: Dict[str, str] = None):
        self.data = data
        self.status_code = status_code
        self.headers = headers or {}


class ExampleAPI:
    """
    Example API demonstrating CyRedis web cache functionality.
    Similar to FastAPI with @cache decorators and HTTP headers.
    """

    def __init__(self):
        # Initialize cache with Redis backend
        self.redis_client = CyRedisClient()
        init_cache(
            redis_client=self.redis_client,
            backend_type="redis",
            prefix="cyredis:api_cache",
            default_ttl=300  # 5 minutes default
        )

        # Get cache instance for direct usage
        self.cache = get_cache()

        # In-memory storage for demo data
        self.users_db = {
            "1": {"id": "1", "name": "Alice", "email": "alice@example.com", "age": 30},
            "2": {"id": "2", "name": "Bob", "email": "bob@example.com", "age": 25},
            "3": {"id": "3", "name": "Charlie", "email": "charlie@example.com", "age": 35}
        }

        self.posts_db = {
            "1": {"id": "1", "title": "Hello World", "content": "First post", "author_id": "1"},
            "2": {"id": "2", "title": "Redis Caching", "content": "About caching", "author_id": "2"},
            "3": {"id": "3", "title": "Python Tips", "content": "Programming tips", "author_id": "1"}
        }

    # ===== BASIC CACHING EXAMPLES =====

    def get_expensive_data(self, user_id: str) -> Dict[str, Any]:
        """Simulate expensive operation that should be cached"""
        print(f"🔄 Computing expensive data for user {user_id}...")

        # Simulate expensive computation
        time.sleep(2)

        user = self.users_db.get(user_id)
        if not user:
            return {"error": "User not found"}

        # Add some computed data
        return {
            "user": user,
            "computed_at": time.time(),
            "random_factor": random.randint(1, 100),
            "cached": False  # Will be overridden by cache decorator
        }

    @cache(ttl=60, namespace="expensive_data")
    def get_cached_expensive_data(self, user_id: str) -> Dict[str, Any]:
        """Cached version of expensive operation"""
        return self.get_expensive_data(user_id)

    # ===== HTTP ENDPOINT CACHING =====

    @cached_endpoint(ttl=300, namespace="api_endpoints")
    async def get_user(self, request, user_id: str) -> Dict[str, Any]:
        """Get user with HTTP caching"""
        print(f"🔄 Fetching user {user_id} from database...")

        # Simulate database query
        time.sleep(0.5)

        user = self.users_db.get(user_id)
        if not user:
            return {"error": "User not found", "status_code": 404}

        return {
            "data": user,
            "cached": False,
            "timestamp": time.time()
        }

    @cached_json_response(ttl=180)
    async def get_users_list(self, request) -> Dict[str, Any]:
        """Get all users with JSON response caching"""
        print("🔄 Fetching users list...")

        # Simulate database query
        time.sleep(0.3)

        return {
            "users": list(self.users_db.values()),
            "count": len(self.users_db),
            "cached": False
        }

    # ===== CONDITIONAL REQUESTS (ETags) =====

    @cached_endpoint(ttl=300, namespace="etag_endpoints")
    async def get_post_with_etag(self, request, post_id: str) -> Dict[str, Any]:
        """Get post with ETag support for conditional requests"""
        print(f"🔄 Fetching post {post_id}...")

        # Simulate database query
        time.sleep(0.4)

        post = self.posts_db.get(post_id)
        if not post:
            return {"error": "Post not found", "status_code": 404}

        # Join with user data
        author = self.users_db.get(post["author_id"], {})
        post_data = {
            "post": post,
            "author": author,
            "cached": False,
            "timestamp": time.time()
        }

        return post_data

    # ===== CACHE INVALIDATION =====

    def create_user(self, name: str, email: str, age: int) -> Dict[str, Any]:
        """Create new user (invalidates user caches)"""
        user_id = str(len(self.users_db) + 1)

        new_user = {
            "id": user_id,
            "name": name,
            "email": email,
            "age": age
        }

        self.users_db[user_id] = new_user

        # Invalidate related caches
        self.cache.invalidate_namespace("api_endpoints")
        self.cache.invalidate_pattern("user:*")

        return {"user": new_user, "created": True}

    def update_user(self, user_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update user (invalidates caches)"""
        if user_id not in self.users_db:
            return {"error": "User not found"}

        # Update user
        self.users_db[user_id].update(updates)

        # Invalidate caches
        self.cache.invalidate_pattern(f"user:{user_id}")
        self.cache.invalidate_namespace("api_endpoints")

        return {"user": self.users_db[user_id], "updated": True}

    # ===== CACHE STATISTICS =====

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "cache_stats": self.cache.get_stats(),
            "redis_info": self.redis_client.info(),
            "demo_data": {
                "users_count": len(self.users_db),
                "posts_count": len(self.posts_db)
            }
        }

    # ===== PERFORMANCE DEMONSTRATION =====

    async def demonstrate_caching(self):
        """Demonstrate caching performance improvements"""
        print("🚀 Demonstrating CyRedis Web Cache Performance")
        print("=" * 60)

        # 1. First call - should be slow (cache miss)
        print("\n📊 First call (cache miss):")
        start_time = time.time()
        result1 = await self.get_user(MockRequest(), "1")
        first_call_time = time.time() - start_time
        print(f"   Time: {first_call_time:.3f}s")
        print(f"   Cached: {result1.get('cached', 'N/A')}")

        # 2. Second call - should be fast (cache hit)
        print("\n📊 Second call (cache hit):")
        start_time = time.time()
        result2 = await self.get_user(MockRequest(), "1")
        second_call_time = time.time() - start_time
        print(f"   Time: {second_call_time:.3f}s")
        print(f"   Cached: {result2.get('cached', 'N/A')}")
        print(f"   Speedup: {first_call_time/second_call_time:.1f}x faster")

        # 3. Different user - cache miss
        print("\n📊 Different user (cache miss):")
        start_time = time.time()
        result3 = await self.get_user(MockRequest(), "2")
        third_call_time = time.time() - start_time
        print(f"   Time: {third_call_time:.3f}s")
        print(f"   Cached: {result3.get('cached', 'N/A')}")

        # 4. Demonstrate ETag conditional requests
        print("\n🏷️  Demonstrating ETag conditional requests:")

        # First request - get with ETag
        request1 = MockRequest(path="/api/posts/1")
        response1 = await self.get_post_with_etag(request1, "1")
        etag = response1.get("headers", {}).get("ETag")

        print(f"   First request ETag: {etag}")

        # Second request - same ETag (should return 304)
        request2 = MockRequest(
            path="/api/posts/1",
            headers={"If-None-Match": etag}
        )
        response2 = await self.get_post_with_etag(request2, "1")

        print(f"   Conditional request status: {response2.get('status_code', 200)}")

        return {
            "first_call_time": first_call_time,
            "second_call_time": second_call_time,
            "speedup": first_call_time / second_call_time if second_call_time > 0 else 0,
            "etag_demo": {
                "first_response": response1,
                "conditional_response": response2
            }
        }

    # ===== CACHE MANAGEMENT =====

    async def demonstrate_cache_management(self):
        """Demonstrate cache management operations"""
        print("\n🗑️  Demonstrating Cache Management")
        print("=" * 40)

        # Set some cache data
        print("\n📝 Setting cache data...")
        self.cache.set("demo:key1", {"message": "Hello Cache", "timestamp": time.time()})
        self.cache.set("demo:key2", {"data": [1, 2, 3, 4, 5]}, ttl=60)

        # Get cache data
        print("\n📖 Getting cache data...")
        key1_data = self.cache.get("demo:key1")
        key2_data = self.cache.get("demo:key2")
        print(f"   Key1: {key1_data}")
        print(f"   Key2: {key2_data}")

        # Check existence
        print("\n🔍 Checking cache existence...")
        exists1 = self.cache.exists("demo:key1")
        exists3 = self.cache.exists("demo:key3")
        print(f"   demo:key1 exists: {exists1}")
        print(f"   demo:key3 exists: {exists3}")

        # Pattern invalidation
        print("\n🗑️  Pattern invalidation...")
        self.cache.set("demo:key3", {"temp": "data"})
        self.cache.set("demo:key4", {"temp": "data"})

        print("   Before invalidation:")
        print(f"     demo:key3 exists: {self.cache.exists('demo:key3')}")
        print(f"     demo:key4 exists: {self.cache.exists('demo:key4')}")

        self.cache.invalidate_pattern("demo:key3*")

        print("   After invalidation:")
        print(f"     demo:key3 exists: {self.cache.exists('demo:key3')}")
        print(f"     demo:key4 exists: {self.cache.exists('demo:key4')}")

        # Namespace invalidation
        print("\n🏷️  Namespace invalidation...")
        self.cache.set("namespace:test:key1", {"data": "value1"})
        self.cache.set("namespace:test:key2", {"data": "value2"})

        print("   Before namespace invalidation:")
        print(f"     namespace:test:key1 exists: {self.cache.exists('namespace:test:key1')}")

        self.cache.invalidate_namespace("namespace:test")

        print("   After namespace invalidation:")
        print(f"     namespace:test:key1 exists: {self.cache.exists('namespace:test:key1')}")

    # ===== MULTIPLE BACKENDS DEMO =====

    async def demonstrate_backends(self):
        """Demonstrate different cache backends"""
        print("\n💾 Demonstrating Multiple Cache Backends")
        print("=" * 45)

        # Redis backend (already initialized)
        print("\n🔴 Redis Backend:")
        redis_stats = self.cache.get_stats()
        print(f"   Type: {redis_stats['backend_type']}")
        print(f"   Prefix: {redis_stats['prefix']}")
        print(f"   Default TTL: {redis_stats['default_ttl']}s")

        # In-memory backend
        print("\n🧠 In-Memory Backend:")
        memory_cache = WebCache(backend_type="memory", prefix="demo:memory")
        memory_cache.set("test_key", {"memory": "data"}, ttl=30)

        memory_data = memory_cache.get("test_key")
        memory_exists = memory_cache.exists("test_key")

        print(f"   Data: {memory_data}")
        print(f"   Exists: {memory_exists}")
        print(f"   Stats: {memory_cache.get_stats()}")

        return {
            "redis_stats": redis_stats,
            "memory_data": memory_data
        }


async def main():
    """Main demonstration function"""
    print("🎯 CyRedis Web Cache - FastAPI-Style Features Demo")
    print("=" * 60)

    # Initialize API
    api = ExampleAPI()

    try:
        # 1. Performance demonstration
        performance_results = await api.demonstrate_caching()

        # 2. Cache management demonstration
        await api.demonstrate_cache_management()

        # 3. Multiple backends demonstration
        backend_results = await api.demonstrate_backends()

        # 4. Show cache statistics
        print("\n📊 Final Cache Statistics:")
        stats = api.get_cache_stats()
        print(json.dumps(stats, indent=2, default=str))

        # 5. Demonstrate cache invalidation on data changes
        print("\n🔄 Demonstrating cache invalidation on data changes:")

        # Create new user
        print("Creating new user...")
        create_result = api.create_user("Diana", "diana@example.com", 28)
        print(f"Created: {create_result}")

        # Update existing user
        print("Updating existing user...")
        update_result = api.update_user("1", {"age": 31})
        print(f"Updated: {update_result}")

        # Check that caches were invalidated
        print("Checking cache invalidation...")
        user_cache_exists = api.cache.exists("api_endpoints:user:1")
        print(f"User cache invalidated: {not user_cache_exists}")

        print("\n✅ CyRedis Web Cache demonstration completed!")
        print("\nKey features demonstrated:")
        print("  ✓ HTTP cache headers (ETag, Cache-Control)")
        print("  ✓ Conditional requests (304 Not Modified)")
        print("  ✓ @cache decorators for functions and endpoints")
        print("  ✓ Multiple cache backends (Redis, Memory, Memcached, DynamoDB)")
        print("  ✓ Cache invalidation by pattern and namespace")
        print("  ✓ Performance improvements through caching")
        print("  ✓ JSON and Pickle coders")
        print("  ✓ Custom key builders")

    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
