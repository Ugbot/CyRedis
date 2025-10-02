#!/usr/bin/env python3
"""
Advanced Redis Client - High-performance Redis with compression, bulk ops, and monitoring
"""

try:
    from cy_redis.advanced import CyAdvancedRedisClient as AdvancedRedisClient
    _ADVANCED_AVAILABLE = True
    print("✓ Advanced CyRedis features loaded")
except ImportError:
    _ADVANCED_AVAILABLE = False
    print("⚠️  Advanced CyRedis features not available, using fallback")

from optimized_redis import OptimizedRedis


class AdvancedRedis:
    """
    Advanced Redis client with compression, bulk operations, metrics, and circuit breakers.
    """

    def __init__(self, **kwargs):
        if _ADVANCED_AVAILABLE:
            # Use optimized Cython implementation
            redis_client = OptimizedRedis(**kwargs).client
            self._impl = AdvancedRedisClient(redis_client)
        else:
            # Fallback to basic optimized client
            self._impl = OptimizedRedis(**kwargs)

    # Core operations with advanced features
    def get(self, key):
        """Get with compression and metrics."""
        return self._impl.get(key)

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set with compression and metrics."""
        return self._impl.set(key, value)

    def delete(self, key):
        """Delete operation."""
        return self._impl.delete(key)

    # Bulk operations
    def mget(self, keys):
        """Bulk get with optimizations."""
        if hasattr(self._impl, 'mget'):
            return self._impl.mget(keys)
        else:
            # Fallback to individual gets
            return [self.get(key) for key in keys]

    def mset(self, data):
        """Bulk set with optimizations."""
        if hasattr(self._impl, 'bulk_ops'):
            self._impl.bulk_ops.mset_bulk(data)
            return "OK"
        else:
            # Fallback to individual sets
            for key, value in data.items():
                self.set(key, value)
            return "OK"

    # Advanced features
    def get_metrics(self):
        """Get comprehensive metrics."""
        if hasattr(self._impl, 'get_metrics'):
            return self._impl.get_metrics()
        return {"status": "metrics not available"}

    def get_circuit_breaker_state(self):
        """Get circuit breaker state."""
        if hasattr(self._impl, 'get_circuit_breaker_state'):
            return self._impl.get_circuit_breaker_state()
        return {"status": "circuit breaker not available"}

    # Async versions
    async def get_async(self, key):
        """Async get with full optimizations."""
        if hasattr(self._impl, 'get_async'):
            return await self._impl.get_async(key)
        else:
            # Fallback - this would need proper async implementation
            raise NotImplementedError("Async not available in fallback mode")

    async def set_async(self, key, value):
        """Async set with full optimizations."""
        if hasattr(self._impl, 'set_async'):
            return await self._impl.set_async(key, value)
        else:
            raise NotImplementedError("Async not available in fallback mode")

    async def mget_async(self, keys):
        """Async bulk get."""
        if hasattr(self._impl, 'mget_async'):
            return await self._impl.mget_async(keys)
        else:
            raise NotImplementedError("Async not available in fallback mode")


# Convenience function to create advanced client
def create_advanced_client(**kwargs):
    """Create an advanced Redis client with all optimizations."""
    return AdvancedRedis(**kwargs)
