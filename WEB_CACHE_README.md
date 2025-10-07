# CyRedis Web Cache - FastAPI-Style Features

This document describes the new web cache functionality in CyRedis, which provides FastAPI-cache inspired features for high-performance HTTP caching, decorators, multiple backends, and conditional requests.

## 🚀 Overview

CyRedis now includes a comprehensive web cache system that borrows the best features from [fastapi-cache](https://github.com/long2ice/fastapi-cache) while leveraging Redis for high-performance, distributed operations:

- **HTTP Cache Headers**: ETag, Cache-Control, If-None-Match support
- **Decorators**: `@cache`, `@cached_endpoint`, `@cached_json_response`
- **Multiple Backends**: Redis, In-Memory, Memcached, DynamoDB
- **Coders**: JSON, Pickle, and custom encoders/decoders
- **Key Builders**: Custom cache key generation strategies
- **Conditional Requests**: 304 Not Modified responses
- **Cache Management**: Pattern and namespace invalidation

## 📁 Core Components

### 1. WebCache Class

The main entry point for web caching functionality:

```python
from cy_redis.web_cache import WebCache, init_cache, get_cache

# Initialize with Redis backend (default)
cache = WebCache(backend_type="redis", prefix="myapp:cache", default_ttl=300)

# Or use global instance
init_cache(backend_type="redis", prefix="myapp:cache")
cache = get_cache()

# Basic operations
cache.set("key", {"data": "value"}, ttl=300)
data = cache.get("key")
cache.delete("key")
```

### 2. Cache Backends

Multiple backend support for different use cases:

#### Redis Backend (Default)
```python
from cy_redis.cy_redis_client import CyRedisClient
from cy_redis.web_cache import create_redis_cache

redis_client = CyRedisClient()
cache = create_redis_cache(redis_client, prefix="myapp:cache", default_ttl=300)
```

#### In-Memory Backend
```python
from cy_redis.web_cache import create_memory_cache

cache = create_memory_cache(prefix="myapp:cache", default_ttl=300)
```

#### Memcached Backend
```python
from cy_redis.web_cache import create_memcached_cache

cache = create_memcached_cache(
    host="localhost", port=11211,
    prefix="myapp:cache", default_ttl=300
)
```

#### DynamoDB Backend
```python
from cy_redis.web_cache import create_dynamodb_cache

cache = create_dynamodb_cache(
    table_name="MyCacheTable",
    region_name="us-east-1",
    prefix="myapp:cache", default_ttl=300
)
```

### 3. Cache Decorators

#### @cache - Function Caching
```python
from cy_redis.web_cache import cache

@cache(ttl=60, namespace="expensive_operations")
def get_user_data(user_id: str) -> Dict[str, Any]:
    # Expensive database query or computation
    return {"user_id": user_id, "data": "expensive_result"}
```

#### @cached_endpoint - HTTP Endpoint Caching
```python
from cy_redis.web_cache import cached_endpoint

@cached_endpoint(ttl=300, namespace="api_endpoints")
async def get_user(request, user_id: str) -> Dict[str, Any]:
    # Database query with automatic HTTP caching
    user = await database.get_user(user_id)
    return {"user": user, "timestamp": time.time()}
```

#### @cached_json_response - JSON API Caching
```python
from cy_redis.web_cache import cached_json_response

@cached_json_response(ttl=180)
async def get_users_list(request) -> Dict[str, Any]:
    # List endpoint with automatic caching
    users = await database.get_all_users()
    return {"users": users, "count": len(users)}
```

## 🏷️ HTTP Cache Headers

Automatic HTTP cache header management:

```python
@cached_endpoint(ttl=300)
async def get_post(request, post_id: str) -> Dict[str, Any]:
    post = await database.get_post(post_id)

    # Automatically adds:
    # - ETag header with content hash
    # - Cache-Control header with max-age
    # - Supports If-None-Match conditional requests
    # - Returns 304 Not Modified when appropriate

    return {"post": post}
```

### Manual Header Management

```python
from cy_redis.web_cache import HTTPCacheHeaders

headers = HTTPCacheHeaders()
response = {"data": "content"}

# Add ETag
response = headers.set_etag_header(response, '"abc123"')

# Add Cache-Control
response = headers.set_cache_headers(response, max_age=3600)

# Check conditional request
is_conditional = headers.check_conditional_request(
    {"If-None-Match": '"abc123"'},
    '"abc123"'
)

if is_conditional:
    return headers.create_304_response()
```

## 🔑 Cache Key Builders

Custom cache key generation strategies:

### Default Key Builder
```python
from cy_redis.web_cache import DefaultKeyBuilder

# Uses function name, module, and arguments
key = DefaultKeyBuilder.build_key(func, namespace="api", user_id="123")
# Result: "api:myapp.api.get_user:user_id=123"
```

### Request-Based Key Builder
```python
from cy_redis.web_cache import RequestKeyBuilder

# Uses HTTP request method, path, and query params
key = RequestKeyBuilder.build_key(
    func, namespace="api",
    request=MockRequest(method="GET", path="/api/users", query_params=[("limit", "10")])
)
# Result: "api:GET:/api/users:[('limit', '10')]"
```

## 📦 Cache Coders

Different data serialization strategies:

### JSON Coder (Default)
```python
from cy_redis.web_cache import JsonCoder

# Automatic JSON encoding/decoding
encoded = JsonCoder.encode({"key": "value", "number": 42})
# encoded = b'{"key": "value", "number": 42}'

decoded = JsonCoder.decode(encoded)
# decoded = {"key": "value", "number": 42}
```

### Pickle Coder
```python
from cy_redis.web_cache import PickleCoder

# For complex Python objects
class CustomObject:
    def __init__(self, data):
        self.data = data

obj = CustomObject("test data")
encoded = PickleCoder.encode(obj)
decoded = PickleCoder.decode(encoded)
# decoded is CustomObject instance
```

## 🔄 Cache Invalidation

Smart cache invalidation strategies:

### Pattern Invalidation
```python
# Invalidate all keys matching pattern
cache.invalidate_pattern("user:*")        # All user keys
cache.invalidate_pattern("api:posts:*")   # All post API keys
```

### Namespace Invalidation
```python
# Invalidate entire namespace
cache.invalidate_namespace("api_endpoints")  # All API endpoint caches
cache.invalidate_namespace("user_profiles")   # All user profile caches
```

### Automatic Invalidation
```python
def update_user(user_id: str, updates: Dict[str, Any]):
    # Update database
    database.update_user(user_id, updates)

    # Invalidate related caches
    cache.invalidate_pattern(f"user:{user_id}:*")
    cache.invalidate_namespace("api_endpoints")
```

## 🎯 FastAPI Integration

Integration with FastAPI applications:

```python
from fastapi import FastAPI, Request, Depends
from cy_redis.web_cache import WebCache, cached_endpoint

app = FastAPI()

# Initialize cache
cache = WebCache()

@app.on_event("startup")
async def startup():
    # Cache initialization handled automatically
    pass

@app.get("/users/{user_id}")
@cached_endpoint(ttl=300, namespace="api_endpoints")
async def get_user(request: Request, user_id: str):
    # This endpoint is automatically cached
    user = await database.get_user(user_id)
    return {"user": user}

@app.get("/posts")
@cached_json_response(ttl=180)
async def get_posts(request: Request, limit: int = 10):
    # JSON API with automatic caching
    posts = await database.get_posts(limit=limit)
    return {"posts": posts, "count": len(posts)}

@app.post("/users/{user_id}")
async def update_user_endpoint(user_id: str, updates: Dict[str, Any]):
    # Update user and invalidate caches
    result = await database.update_user(user_id, updates)

    # Invalidate related caches
    cache.invalidate_pattern(f"user:{user_id}:*")
    cache.invalidate_namespace("api_endpoints")

    return result
```

## 🚀 Performance Features

### Conditional Requests
```python
# First request - gets full response with ETag
response = await get_post(request, "123")
etag = response["headers"]["ETag"]  # "abc123"

# Second request - same ETag returns 304
response = await get_post(
    MockRequest(headers={"If-None-Match": etag}),
    "123"
)
# response.status_code == 304
```

### Cache Statistics
```python
stats = cache.get_stats()
print(f"Backend: {stats['backend_type']}")
print(f"Hit Rate: {stats.get('hit_rate', 'N/A')}")
print(f"Keys Count: {stats.get('keys_count', 'N/A')}")
```

## ⚙️ Configuration

### Backend Configuration

```python
# Redis with custom settings
cache = WebCache(
    backend_type="redis",
    backend_config={
        "host": "redis-cluster.example.com",
        "port": 6379,
        "password": "secret"
    }
)

# DynamoDB with custom settings
cache = WebCache(
    backend_type="dynamodb",
    backend_config={
        "table_name": "MyAppCache",
        "region_name": "us-west-2",
        "prefix": "production:cache"
    }
)
```

### Global Configuration

```python
from cy_redis.web_cache import init_cache

# Configure global cache instance
init_cache(
    backend_type="redis",
    prefix="myapp:cache",
    default_ttl=300,
    backend_config={
        "host": "localhost",
        "port": 6379
    }
)
```

## 📊 Monitoring and Debugging

### Cache Inspection
```python
# Get cache statistics
stats = cache.get_stats()

# List all keys (Redis only)
keys = cache.backend._get_client().keys(f"{cache.prefix}:*")

# Check specific key
exists = cache.exists("api_endpoints:user:123")
```

### Performance Monitoring
```python
import time

# Measure cache performance
start = time.time()
data = cache.get("expensive_key")
cache_time = time.time() - start

if data:
    print(f"Cache hit in {cache_time:.3f}s")
else:
    print("Cache miss - computing data...")
```

## 🔧 Best Practices

### 1. Key Design
```python
# Good: Include all relevant parameters
@cache(namespace="user_profile")
def get_user_profile(user_id: str, include_preferences: bool = True):
    pass

# Avoid: Non-deterministic keys
@cache()  # Bad - no parameters for unique identification
def get_random_data():
    return random.randint(1, 100)
```

### 2. TTL Strategy
```python
# Short TTL for frequently changing data
@cache(ttl=60, namespace="realtime")
def get_live_stats():
    pass

# Long TTL for static data
@cache(ttl=3600, namespace="static")
def get_country_list():
    pass

# No TTL for permanent data
@cache(ttl=None, namespace="permanent")
def get_system_config():
    pass
```

### 3. Invalidation Strategy
```python
def update_user(user_id: str, data: Dict[str, Any]):
    # Update database
    database.update(user_id, data)

    # Invalidate all related caches
    cache.invalidate_pattern(f"user:{user_id}:*")
    cache.invalidate_namespace("user_endpoints")
    cache.invalidate_namespace("dashboard_data")
```

### 4. Error Handling
```python
@cache(ttl=300)
def get_user_data(user_id: str):
    try:
        return database.get_user(user_id)
    except DatabaseError:
        # Don't cache errors
        raise
    except Exception as e:
        # Log error but don't cache it
        logger.error(f"Unexpected error: {e}")
        raise
```

## 🎉 Advanced Features

### Custom Coders
```python
class CustomCoder:
    @classmethod
    def encode(cls, value):
        # Custom serialization logic
        return msgpack.packb(value)

    @classmethod
    def decode(cls, value):
        # Custom deserialization logic
        return msgpack.unpackb(value)

# Use custom coder
@cache(coder=CustomCoder, ttl=300)
def get_custom_data():
    return {"custom": "object"}
```

### Custom Key Builders
```python
class UserKeyBuilder:
    @classmethod
    def build_key(cls, func, namespace="", user_id=None, **kwargs):
        # Include user context in key
        user_context = f"user:{user_id}" if user_id else "anonymous"
        return f"{namespace}:{func.__name__}:{user_context}"

@cache(key_builder=UserKeyBuilder, namespace="user_data")
def get_user_specific_data(user_id: str):
    pass
```

## 📈 Performance Benefits

The CyRedis web cache provides significant performance improvements:

- **Response Time**: 10-100x faster for cached responses
- **Database Load**: Reduced by caching frequently accessed data
- **Bandwidth**: Conditional requests save bandwidth
- **Scalability**: Distributed cache across multiple instances
- **Reliability**: Automatic fallback and error handling

## 🔗 Integration Examples

### Django Integration
```python
# settings.py
CACHES = {
    'default': {
        'BACKEND': 'cy_redis.web_cache.WebCache',
        'LOCATION': 'redis://127.0.0.1:6379/1',
    }
}

# views.py
from cy_redis.web_cache import cache

@cache(ttl=300)
def get_expensive_data(user_id):
    return database.get_user_stats(user_id)
```

### Flask Integration
```python
from flask import Flask
from cy_redis.web_cache import WebCache

app = Flask(__name__)
cache = WebCache()

@app.route('/api/users/<user_id>')
@cache.cached_endpoint(ttl=300)
def get_user(user_id):
    user = database.get_user(user_id)
    return {"user": user}
```

## 🎯 Conclusion

CyRedis Web Cache brings FastAPI-cache inspired functionality to Redis-based applications, providing:

- **High Performance**: Sub-millisecond cache operations
- **Scalability**: Distributed cache across multiple instances
- **Flexibility**: Multiple backends and customization options
- **Reliability**: Automatic error handling and fallbacks
- **Ease of Use**: Simple decorators and intuitive API

This cache system enables you to build fast, scalable web applications with minimal code changes while maintaining the performance and reliability benefits of Redis caching.
