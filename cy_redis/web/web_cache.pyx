# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Web Cache Module for CyRedis
Provides FastAPI-cache inspired functionality including HTTP cache headers,
decorators, multiple backends, and conditional requests.
"""

import asyncio
import json
import time
import hashlib
import hmac
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from datetime import datetime, timedelta
import os
import signal
import sys

# Import core Redis functionality
from cy_redis.core.cy_redis_client import CyRedisClient
from cy_redis.features.distributed import CyDistributedLock


# ===== HTTP CACHE HEADERS AND CONDITIONAL REQUESTS =====

cdef class HTTPCacheHeaders:
    """
    HTTP cache headers management with ETag and Cache-Control support.
    """

    def __cinit__(self, str etag_header_name="ETag", str cache_control_header="Cache-Control"):
        self.etag_header_name = etag_header_name
        self.cache_control_header = cache_control_header

    def generate_etag(self, content: Any) -> str:
        """Generate ETag from content"""
        content_str = json.dumps(content, sort_keys=True, default=str)
        etag_data = hashlib.md5(content_str.encode()).hexdigest()
        # md5 hexdigest is always 32 hex chars; the quoted ETag is therefore 34.
        assert len(etag_data) == 32, "md5 hexdigest must be 32 chars"
        etag = f'"{etag_data}"'
        assert etag.startswith('"') and etag.endswith('"'), "ETag must be quoted"
        return etag

    def set_cache_headers(self, response: Dict[str, Any], max_age: int = 3600,
                         cache_control: str = None) -> Dict[str, Any]:
        """Set cache headers on response"""
        # Precondition: a negative max-age is not a representable HTTP value.
        if max_age < 0:
            raise ValueError("max_age must be non-negative")

        headers = response.get('headers', {})

        # Set Cache-Control header
        if cache_control:
            headers[self.cache_control_header] = cache_control
        else:
            headers[self.cache_control_header] = f"max-age={max_age}"

        response['headers'] = headers
        # Postcondition: the response now carries a Cache-Control header.
        assert self.cache_control_header in response['headers'], \
            "Cache-Control header must be set"
        return response

    def set_etag_header(self, response: Dict[str, Any], etag: str) -> Dict[str, Any]:
        """Set ETag header on response"""
        headers = response.get('headers', {})
        headers[self.etag_header_name] = etag
        response['headers'] = headers
        return response

    def check_conditional_request(self, request_headers: Dict[str, str],
                                current_etag: str) -> bool:
        """Check if request has matching ETag for conditional request"""
        if_none_match = request_headers.get('If-None-Match')
        if if_none_match and if_none_match == current_etag:
            return True
        return False

    def create_304_response(self) -> Dict[str, Any]:
        """Create 304 Not Modified response"""
        return {
            'status_code': 304,
            'headers': {
                self.cache_control_header: "max-age=3600"
            }
        }


# ===== CACHE CODERS =====

cdef class Coder:
    """Base class for cache data coders"""

    @classmethod
    def encode(cls, value: Any) -> bytes:
        """Encode value to bytes for caching"""
        raise NotImplementedError

    @classmethod
    def decode(cls, value: bytes) -> Any:
        """Decode bytes back to original value"""
        raise NotImplementedError


cdef class JsonCoder(Coder):
    """JSON coder for cache data"""

    @classmethod
    def encode(cls, value: Any) -> bytes:
        """Encode to JSON bytes"""
        encoded = json.dumps(value, default=str).encode('utf-8')
        # Postcondition: the cache backend stores raw bytes.
        assert isinstance(encoded, bytes), "encode must yield bytes"
        return encoded

    @classmethod
    def decode(cls, value: bytes) -> Any:
        """Decode from JSON bytes"""
        # Precondition: only bytes round-trip through a cache backend.
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError("JsonCoder.decode expects bytes")
        return json.loads(value.decode('utf-8'))


cdef class PickleCoder(Coder):
    """Pickle coder for cache data"""

    @classmethod
    def encode(cls, value: Any) -> bytes:
        """Encode to pickle bytes"""
        import pickle
        encoded = pickle.dumps(value)
        # Postcondition: the cache backend stores raw bytes.
        assert isinstance(encoded, bytes), "encode must yield bytes"
        return encoded

    @classmethod
    def decode(cls, value: bytes) -> Any:
        """Decode from pickle bytes"""
        # Precondition: only bytes round-trip through a cache backend.
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError("PickleCoder.decode expects bytes")
        import pickle
        # SECURITY NOTE (not fixed here per task scope): pickle.loads on cached
        # values is an RCE vector if an attacker can write to the cache backend.
        # A trusted-source or signed-payload guard belongs here.
        return pickle.loads(value)


# ===== CACHE KEY BUILDERS =====

cdef class KeyBuilder:
    """Base class for cache key builders"""

    @classmethod
    def build_key(cls, func, namespace: str = "", **kwargs) -> str:
        """Build cache key from function and arguments"""
        raise NotImplementedError


cdef class DefaultKeyBuilder(KeyBuilder):
    """Default key builder using function name and arguments"""

    @classmethod
    def build_key(cls, func, namespace: str = "", **kwargs) -> str:
        """Build key from function module, name, and arguments"""
        # Precondition: the decorated target must be a real callable.
        if func is None:
            raise ValueError("func is required to build a cache key")
        func_name = f"{func.__module__}.{func.__name__}"
        args_repr = repr(sorted(kwargs.items())) if kwargs else ""
        key_data = f"{namespace}:{func_name}:{args_repr}"
        cache_key = hashlib.md5(key_data.encode()).hexdigest()
        assert len(cache_key) == 32, "cache key must be a 32-char md5 digest"
        return cache_key


cdef class RequestKeyBuilder(KeyBuilder):
    """Key builder using HTTP request data"""

    @classmethod
    def build_key(cls, func, namespace: str = "", request=None, **kwargs) -> str:
        """Build key from HTTP request method, path, and query params"""
        if not request:
            return DefaultKeyBuilder.build_key(func, namespace, **kwargs)

        key_parts = [
            namespace,
            request.method.lower(),
            request.url.path,
            str(sorted(request.query_params.items()))
        ]
        key_data = ":".join(key_parts)
        cache_key = hashlib.md5(key_data.encode()).hexdigest()
        assert len(cache_key) == 32, "cache key must be a 32-char md5 digest"
        return cache_key


# ===== CACHE BACKENDS =====

cdef class CacheBackend:
    """Base class for cache backends"""

    def __cinit__(self):
        self.prefix = "cyredis:cache"

    def get(self, key: str) -> Optional[bytes]:
        """Get value from cache"""
        raise NotImplementedError

    def set(self, key: str, value: bytes, ttl: int = None):
        """Set value in cache with optional TTL"""
        raise NotImplementedError

    def delete(self, key: str) -> bool:
        """Delete value from cache"""
        raise NotImplementedError

    def clear(self, pattern: str = None):
        """Clear cache, optionally by pattern"""
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        raise NotImplementedError

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on existing key"""
        raise NotImplementedError


cdef class RedisCacheBackend(CacheBackend):
    """Redis cache backend"""

    def __cinit__(self, redis_client, str prefix="cyredis:cache"):
        self.redis_client = redis_client
        self.prefix = prefix

    def _make_key(self, key: str) -> str:
        """Create prefixed key"""
        return f"{self.prefix}:{key}"

    def get(self, key: str) -> Optional[bytes]:
        """Get value from Redis cache"""
        redis_key = self._make_key(key)
        value = self.redis_client.get(redis_key)
        return value.encode('utf-8') if value else None

    def set(self, key: str, value: bytes, ttl: int = None):
        """Set value in Redis cache"""
        redis_key = self._make_key(key)
        if ttl:
            self.redis_client.set(redis_key, value.decode('utf-8'), ex=ttl)
        else:
            self.redis_client.set(redis_key, value.decode('utf-8'))

    def delete(self, key: str) -> bool:
        """Delete value from Redis cache"""
        redis_key = self._make_key(key)
        return self.redis_client.delete(redis_key) > 0

    def clear(self, pattern: str = None):
        """Clear Redis cache"""
        if pattern:
            # Use SCAN for pattern matching
            keys = self.redis_client.scan(0, match=f"{self.prefix}:{pattern}*")[1]
            if keys:
                self.redis_client.delete(*keys)
        else:
            # Clear all keys with prefix
            keys = self.redis_client.scan(0, match=f"{self.prefix}:*")[1]
            if keys:
                self.redis_client.delete(*keys)

    def exists(self, key: str) -> bool:
        """Check if key exists in Redis cache"""
        redis_key = self._make_key(key)
        return self.redis_client.exists(redis_key) > 0

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on Redis key"""
        redis_key = self._make_key(key)
        return self.redis_client.expire(redis_key, ttl)


cdef class InMemoryCacheBackend(CacheBackend):
    """In-memory cache backend"""

    def __cinit__(self, str prefix="cyredis:cache"):
        self.prefix = prefix
        self.cache = {}
        self.expiries = {}

    def _make_key(self, key: str) -> str:
        """Create prefixed key"""
        return f"{self.prefix}:{key}"

    def _cleanup_expired(self):
        """Clean up expired entries"""
        current_time = time.time()
        expired_keys = []

        # Both loops are bounded by the current number of tracked expiries.
        # Snapshot the items so mutation below cannot disturb iteration.
        for key, expiry in list(self.expiries.items()):
            if current_time > expiry:
                expired_keys.append(key)

        assert len(expired_keys) <= len(self.expiries), \
            "cannot expire more keys than are tracked"
        for key in expired_keys:
            self.cache.pop(key, None)
            self.expiries.pop(key, None)

    def get(self, key: str) -> Optional[bytes]:
        """Get value from memory cache"""
        self._cleanup_expired()
        cache_key = self._make_key(key)
        value = self.cache.get(cache_key)
        return value.encode('utf-8') if value else None

    def set(self, key: str, value: bytes, ttl: int = None):
        """Set value in memory cache"""
        cache_key = self._make_key(key)
        self.cache[cache_key] = value.decode('utf-8')

        if ttl:
            self.expiries[cache_key] = time.time() + ttl
        elif cache_key in self.expiries:
            del self.expiries[cache_key]

    def delete(self, key: str) -> bool:
        """Delete value from memory cache"""
        cache_key = self._make_key(key)
        deleted = cache_key in self.cache
        self.cache.pop(cache_key, None)
        self.expiries.pop(cache_key, None)
        return deleted

    def clear(self, pattern: str = None):
        """Clear memory cache"""
        if pattern:
            # Simple pattern matching for in-memory
            pattern = f"{self.prefix}:{pattern}*"
            keys_to_delete = []
            for key in self.cache.keys():
                if key.startswith(pattern.replace('*', '')):
                    keys_to_delete.append(key)

            for key in keys_to_delete:
                self.cache.pop(key, None)
                self.expiries.pop(key, None)
        else:
            self.cache.clear()
            self.expiries.clear()

    def exists(self, key: str) -> bool:
        """Check if key exists in memory cache"""
        self._cleanup_expired()
        cache_key = self._make_key(key)
        return cache_key in self.cache

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on memory cache key"""
        cache_key = self._make_key(key)
        if cache_key in self.cache:
            self.expiries[cache_key] = time.time() + ttl
            return True
        return False


cdef class MemcachedCacheBackend(CacheBackend):
    """Memcached cache backend"""

    def __cinit__(self, str host="localhost", int port=11211, str prefix="cyredis:cache"):
        self.prefix = prefix
        self.host = host
        self.port = port
        self._client = None

    def _get_client(self):
        """Get or create memcached client"""
        if self._client is None:
            try:
                import memcache
                self._client = memcache.Client([f"{self.host}:{self.port}"])
            except ImportError:
                raise ImportError("memcache package is required for MemcachedCacheBackend")
        return self._client

    def _make_key(self, key: str) -> str:
        """Create prefixed key"""
        return f"{self.prefix}:{key}"

    def get(self, key: str) -> Optional[bytes]:
        """Get value from memcached"""
        client = self._get_client()
        cache_key = self._make_key(key)
        value = client.get(cache_key)
        return value.encode('utf-8') if value else None

    def set(self, key: str, value: bytes, ttl: int = None):
        """Set value in memcached"""
        client = self._get_client()
        cache_key = self._make_key(key)
        client.set(cache_key, value.decode('utf-8'), ttl or 0)

    def delete(self, key: str) -> bool:
        """Delete value from memcached"""
        client = self._get_client()
        cache_key = self._make_key(key)
        return client.delete(cache_key)

    def clear(self, pattern: str = None):
        """Clear memcached cache"""
        client = self._get_client()
        if pattern:
            # For pattern clearing, we'd need to scan all keys
            # This is a limitation of memcached
            # For now, we'll flush all (not ideal but functional)
            client.flush_all()
        else:
            client.flush_all()

    def exists(self, key: str) -> bool:
        """Check if key exists in memcached"""
        client = self._get_client()
        cache_key = self._make_key(key)
        value = client.get(cache_key)
        return value is not None

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on memcached key"""
        client = self._get_client()
        cache_key = self._make_key(key)
        value = client.get(cache_key)
        if value is not None:
            return client.set(cache_key, value, ttl)
        return False


cdef class DynamoDBCacheBackend(CacheBackend):
    """DynamoDB cache backend"""

    def __cinit__(self, str table_name, str region_name="us-east-1", str prefix="cyredis:cache"):
        self.prefix = prefix
        self.table_name = table_name
        self.region_name = region_name
        self._client = None
        self._table = None

    def _get_table(self):
        """Get or create DynamoDB table"""
        if self._table is None:
            try:
                import boto3
                from boto3.dynamodb.conditions import Key

                self._client = boto3.resource('dynamodb', region_name=self.region_name)
                self._table = self._client.Table(self.table_name)

                # Ensure table exists with correct schema
                self._ensure_table_exists()

            except ImportError:
                raise ImportError("boto3 package is required for DynamoDBCacheBackend")

        return self._table

    def _ensure_table_exists(self):
        """Ensure DynamoDB table exists with correct schema"""
        try:
            # Try to describe table to check if it exists
            self._table.table_name
        except Exception:
            # Table doesn't exist, create it
            try:
                self._client.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {'AttributeName': 'cache_key', 'KeyType': 'HASH'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'cache_key', 'AttributeType': 'S'}
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )

                # Wait for table to be created
                self._table.meta.client.get_waiter('table_exists').wait(
                    TableName=self.table_name
                )

            except Exception as e:
                print(f"Warning: Could not create DynamoDB table {self.table_name}: {e}")

    def _make_key(self, key: str) -> str:
        """Create prefixed key"""
        return f"{self.prefix}:{key}"

    def get(self, key: str) -> Optional[bytes]:
        """Get value from DynamoDB"""
        table = self._get_table()
        cache_key = self._make_key(key)

        try:
            response = table.get_item(Key={'cache_key': cache_key})

            if 'Item' in response:
                item = response['Item']
                if item.get('expires_at', 0) > time.time():
                    return item['value'].encode('utf-8')
                else:
                    # Expired, delete it
                    table.delete_item(Key={'cache_key': cache_key})

        except Exception as e:
            print(f"DynamoDB get error: {e}")

        return None

    def set(self, key: str, value: bytes, ttl: int = None):
        """Set value in DynamoDB"""
        table = self._get_table()
        cache_key = self._make_key(key)

        try:
            item = {
                'cache_key': cache_key,
                'value': value.decode('utf-8')
            }

            if ttl:
                item['expires_at'] = time.time() + ttl
            else:
                item['expires_at'] = time.time() + 3600  # Default 1 hour

            table.put_item(Item=item)

        except Exception as e:
            print(f"DynamoDB set error: {e}")

    def delete(self, key: str) -> bool:
        """Delete value from DynamoDB"""
        table = self._get_table()
        cache_key = self._make_key(key)

        try:
            table.delete_item(Key={'cache_key': cache_key})
            return True
        except Exception as e:
            print(f"DynamoDB delete error: {e}")
            return False

    def clear(self, pattern: str = None):
        """Clear DynamoDB cache"""
        table = self._get_table()

        try:
            if pattern:
                # Scan for keys matching pattern (expensive operation)
                response = table.scan()
                keys_to_delete = []

                for item in response.get('Items', []):
                    cache_key = item['cache_key']
                    if cache_key.startswith(f"{self.prefix}:{pattern}"):
                        keys_to_delete.append(cache_key)

                # Delete matching keys
                for cache_key in keys_to_delete:
                    table.delete_item(Key={'cache_key': cache_key})
            else:
                # Scan and delete all keys with our prefix
                response = table.scan()
                keys_to_delete = []

                for item in response.get('Items', []):
                    cache_key = item['cache_key']
                    if cache_key.startswith(self.prefix):
                        keys_to_delete.append(cache_key)

                for cache_key in keys_to_delete:
                    table.delete_item(Key={'cache_key': cache_key})

        except Exception as e:
            print(f"DynamoDB clear error: {e}")

    def exists(self, key: str) -> bool:
        """Check if key exists in DynamoDB"""
        table = self._get_table()
        cache_key = self._make_key(key)

        try:
            response = table.get_item(Key={'cache_key': cache_key})
            if 'Item' in response:
                item = response['Item']
                return item.get('expires_at', 0) > time.time()
        except Exception as e:
            print(f"DynamoDB exists error: {e}")

        return False

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on DynamoDB key"""
        table = self._get_table()
        cache_key = self._make_key(key)

        try:
            response = table.update_item(
                Key={'cache_key': cache_key},
                UpdateExpression='SET expires_at = :ttl',
                ExpressionAttributeValues={':ttl': time.time() + ttl}
            )
            return True
        except Exception as e:
            print(f"DynamoDB expire error: {e}")
            return False


# ===== MAIN CACHE MANAGER =====

cdef class CacheManager:
    """
    Main cache manager with FastAPI-cache inspired functionality.
    """

    def __cinit__(self, CacheBackend backend, Coder coder=None,
                  KeyBuilder key_builder=None, str prefix="cyredis:cache",
                  int default_ttl=3600):
        # Precondition: a cache manager is useless without a backend.
        if backend is None:
            raise ValueError("backend is required")
        if default_ttl < 0:
            raise ValueError("default_ttl must be non-negative")

        self.backend = backend
        self.coder = coder or JsonCoder()
        self.key_builder = key_builder or DefaultKeyBuilder()
        self.prefix = prefix
        self.default_ttl = default_ttl
        self.http_headers = HTTPCacheHeaders()

        # Postcondition: the collaborators every method depends on exist.
        assert self.coder is not None, "coder must be set"
        assert self.key_builder is not None, "key_builder must be set"

    def get(self, key: str) -> Any:
        """Get value from cache"""
        try:
            data = self.backend.get(key)
            if data is None:
                return None
            return self.coder.decode(data)
        except Exception:
            return None

    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value in cache"""
        # Precondition: a cache entry must be addressable by a non-empty key.
        if not key:
            raise ValueError("key must be a non-empty string")
        try:
            encoded = self.coder.encode(value)
            assert isinstance(encoded, (bytes, bytearray)), \
                "coder.encode must yield bytes for the backend"
            self.backend.set(key, encoded, ttl or self.default_ttl)
            return True
        except Exception:
            return False

    def delete(self, key: str) -> bool:
        """Delete value from cache"""
        return self.backend.delete(key)

    def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        return self.backend.exists(key)

    def clear(self, pattern: str = None):
        """Clear cache"""
        self.backend.clear(pattern)

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on cache key"""
        return self.backend.expire(key, ttl)

    # ===== DECORATOR-BASED CACHING =====

    def cache(self, ttl: int = None, namespace: str = "", key_builder: KeyBuilder = None,
              coder: Coder = None):
        """
        Decorator for caching function results.
        Similar to fastapi-cache @cache decorator.
        """
        cache_ttl = ttl or self.default_ttl
        cache_namespace = namespace
        cache_key_builder = key_builder or self.key_builder
        cache_coder = coder or self.coder

        def decorator(func):
            # Handle both sync and async functions
            if asyncio.iscoroutinefunction(func):
                async def async_wrapper(*args, **kwargs):
                    # Build cache key
                    cache_key = cache_key_builder.build_key(
                        func, cache_namespace, **kwargs
                    )

                    # Try to get from cache
                    cached_result = self.get(cache_key)
                    if cached_result is not None:
                        return cached_result

                    # Execute function and cache result
                    result = await func(*args, **kwargs)
                    self.set(cache_key, result, cache_ttl)
                    return result

                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    # Build cache key
                    cache_key = cache_key_builder.build_key(
                        func, cache_namespace, **kwargs
                    )

                    # Try to get from cache
                    cached_result = self.get(cache_key)
                    if cached_result is not None:
                        return cached_result

                    # Execute function and cache result
                    result = func(*args, **kwargs)
                    self.set(cache_key, result, cache_ttl)
                    return result

                return sync_wrapper

        return decorator

    # ===== HTTP ENDPOINT CACHING =====

    def cached_endpoint(self, ttl: int = None, namespace: str = "",
                       key_builder: KeyBuilder = None, coder: Coder = None,
                       cache_headers: bool = True):
        """
        Decorator for caching HTTP endpoint responses with headers.
        """
        cache_ttl = ttl or self.default_ttl
        cache_namespace = namespace
        cache_key_builder = key_builder or self.key_builder
        cache_coder = coder or self.coder

        def decorator(func):
            if asyncio.iscoroutinefunction(func):
                async def async_wrapper(*args, **kwargs):
                    # Extract request from kwargs if present
                    request = kwargs.get('request')
                    response_obj = kwargs.get('response')

                    # Build cache key
                    cache_key = cache_key_builder.build_key(
                        func, cache_namespace, request=request, **kwargs
                    )

                    # Try to get from cache
                    cached_response = self.get(cache_key)
                    if cached_response is not None:
                        # Handle conditional requests
                        if request and cache_headers:
                            current_etag = self.http_headers.generate_etag(cached_response)
                            if self.http_headers.check_conditional_request(
                                dict(request.headers), current_etag
                            ):
                                return self.http_headers.create_304_response()

                        return cached_response

                    # Execute function
                    result = await func(*args, **kwargs)

                    # Add cache headers if requested
                    if cache_headers:
                        etag = self.http_headers.generate_etag(result)
                        result = self.http_headers.set_etag_header(result, etag)
                        result = self.http_headers.set_cache_headers(result, cache_ttl)

                    # Cache the result
                    self.set(cache_key, result, cache_ttl)
                    return result

                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    # Extract request from kwargs if present
                    request = kwargs.get('request')

                    # Build cache key
                    cache_key = cache_key_builder.build_key(
                        func, cache_namespace, request=request, **kwargs
                    )

                    # Try to get from cache
                    cached_response = self.get(cache_key)
                    if cached_response is not None:
                        # Handle conditional requests
                        if request and cache_headers:
                            current_etag = self.http_headers.generate_etag(cached_response)
                            if self.http_headers.check_conditional_request(
                                dict(request.headers), current_etag
                            ):
                                return self.http_headers.create_304_response()

                        return cached_response

                    # Execute function
                    result = func(*args, **kwargs)

                    # Add cache headers if requested
                    if cache_headers:
                        etag = self.http_headers.generate_etag(result)
                        result = self.http_headers.set_etag_header(result, etag)
                        result = self.http_headers.set_cache_headers(result, cache_ttl)

                    # Cache the result
                    self.set(cache_key, result, cache_ttl)
                    return result

                return sync_wrapper

        return decorator

    # ===== CACHE MANAGEMENT =====

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'backend_type': type(self.backend).__name__,
            'prefix': self.prefix,
            'default_ttl': self.default_ttl,
            'coder': type(self.coder).__name__,
            'key_builder': type(self.key_builder).__name__
        }

    def invalidate_pattern(self, pattern: str):
        """Invalidate cache entries matching pattern"""
        self.backend.clear(pattern)

    def invalidate_namespace(self, namespace: str):
        """Invalidate all cache entries for a namespace"""
        pattern = f"{namespace}:*"
        self.invalidate_pattern(pattern)


# ===== FACTORY FUNCTIONS =====

def create_redis_cache(redis_client: CyRedisClient, prefix: str = "cyredis:cache",
                      default_ttl: int = 3600) -> CacheManager:
    """Create cache manager with Redis backend"""
    backend = RedisCacheBackend(redis_client, prefix)
    return CacheManager(backend, prefix=prefix, default_ttl=default_ttl)


def create_memory_cache(prefix: str = "cyredis:cache", default_ttl: int = 3600) -> CacheManager:
    """Create cache manager with in-memory backend"""
    backend = InMemoryCacheBackend(prefix)
    return CacheManager(backend, prefix=prefix, default_ttl=default_ttl)


def create_memcached_cache(host: str = "localhost", port: int = 11211,
                          prefix: str = "cyredis:cache", default_ttl: int = 3600) -> CacheManager:
    """Create cache manager with Memcached backend"""
    backend = MemcachedCacheBackend(host, port, prefix)
    return CacheManager(backend, prefix=prefix, default_ttl=default_ttl)


def create_dynamodb_cache(table_name: str, region_name: str = "us-east-1",
                         prefix: str = "cyredis:cache", default_ttl: int = 3600) -> CacheManager:
    """Create cache manager with DynamoDB backend"""
    backend = DynamoDBCacheBackend(table_name, region_name, prefix)
    return CacheManager(backend, prefix=prefix, default_ttl=default_ttl)


# ===== PYTHON WRAPPER CLASS =====

class WebCache:
    """
    Python wrapper for web cache functionality.
    Provides FastAPI-cache inspired API.
    """

    def __init__(self, redis_client: CyRedisClient = None, backend_type: str = "redis",
                 prefix: str = "cyredis:cache", default_ttl: int = 3600,
                 backend_config: Dict[str, Any] = None):
        backend_config = backend_config or {}

        if backend_type == "redis":
            if not redis_client:
                redis_client = CyRedisClient()
            self.cache_manager = create_redis_cache(redis_client, prefix, default_ttl)
        elif backend_type == "memory":
            self.cache_manager = create_memory_cache(prefix, default_ttl)
        elif backend_type == "memcached":
            host = backend_config.get('host', 'localhost')
            port = backend_config.get('port', 11211)
            self.cache_manager = create_memcached_cache(host, port, prefix, default_ttl)
        elif backend_type == "dynamodb":
            table_name = backend_config.get('table_name', 'cyredis_cache')
            region_name = backend_config.get('region_name', 'us-east-1')
            self.cache_manager = create_dynamodb_cache(table_name, region_name, prefix, default_ttl)
        else:
            raise ValueError(f"Unsupported backend type: {backend_type}")

    def get(self, key: str) -> Any:
        """Get value from cache"""
        return self.cache_manager.get(key)

    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value in cache"""
        return self.cache_manager.set(key, value, ttl)

    def delete(self, key: str) -> bool:
        """Delete value from cache"""
        return self.cache_manager.delete(key)

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        return self.cache_manager.exists(key)

    def clear(self, pattern: str = None):
        """Clear cache"""
        self.cache_manager.clear(pattern)

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on key"""
        return self.cache_manager.expire(key, ttl)

    def cache(self, ttl: int = None, namespace: str = "", coder=None):
        """Decorator for caching functions"""
        return self.cache_manager.cache(ttl, namespace, coder=coder)

    def cached_endpoint(self, ttl: int = None, namespace: str = "", coder=None):
        """Decorator for caching HTTP endpoints"""
        return self.cache_manager.cached_endpoint(ttl, namespace, coder=coder)

    def invalidate_pattern(self, pattern: str):
        """Invalidate cache by pattern"""
        self.cache_manager.invalidate_pattern(pattern)

    def invalidate_namespace(self, namespace: str):
        """Invalidate namespace cache"""
        self.cache_manager.invalidate_namespace(namespace)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return self.cache_manager.get_cache_stats()

    # Convenience methods for common use cases
    def cached_json_response(self, ttl: int = 300):
        """Decorator for JSON API responses with caching"""
        def decorator(func):
            if asyncio.iscoroutinefunction(func):
                async def wrapper(*args, **kwargs):
                    # Use request-based key building for better cache efficiency
                    key_builder = RequestKeyBuilder()

                    # Create cache-enabled version
                    cached_func = self.cache_manager.cached_endpoint(
                        ttl=ttl, key_builder=key_builder, cache_headers=True
                    )(func)

                    return await cached_func(*args, **kwargs)
                return wrapper
            else:
                # Sync version
                key_builder = RequestKeyBuilder()
                return self.cache_manager.cached_endpoint(
                    ttl=ttl, key_builder=key_builder, cache_headers=True
                )(func)

        return decorator


# ===== GLOBAL CACHE INSTANCE =====

_global_cache = None

def init_cache(redis_client: CyRedisClient = None, backend_type: str = "redis",
               prefix: str = "cyredis:cache", default_ttl: int = 3600,
               backend_config: Dict[str, Any] = None):
    """Initialize global cache instance"""
    global _global_cache
    _global_cache = WebCache(redis_client, backend_type, prefix, default_ttl, backend_config)


def get_cache() -> WebCache:
    """Get global cache instance"""
    if _global_cache is None:
        init_cache()
    return _global_cache


# Convenience decorators using global cache
def cache(ttl: int = None, namespace: str = ""):
    """Global cache decorator"""
    cache_instance = get_cache()
    return cache_instance.cache(ttl, namespace)


def cached_endpoint(ttl: int = None, namespace: str = ""):
    """Global cached endpoint decorator"""
    cache_instance = get_cache()
    return cache_instance.cached_endpoint(ttl, namespace)


def cached_json_response(ttl: int = 300):
    """Global cached JSON response decorator"""
    cache_instance = get_cache()
    return cache_instance.cached_json_response(ttl)
