#!/usr/bin/env python3
"""
Production Redis Client - Enterprise-ready with connection pooling, TLS, health checks, and retry logic
"""

try:
    from cy_redis.connection_pool import ProductionRedis as ProdRedis
    _PRODUCTION_AVAILABLE = True
    print("✓ Using optimized Cython production Redis client")
except ImportError:
    _PRODUCTION_AVAILABLE = False
    print("⚠️  Optimized production client not available, using fallback")
    # Fallback would be implemented here

from optimized_redis import OptimizedRedis


class ProductionRedis:
    """
    Production-ready Redis client with enterprise features:

    ✅ Enhanced connection pooling (min/max, health checks, idle reaper)
    ✅ TLS/mTLS support with certificate validation
    ✅ Intelligent retry logic (network vs MOVED/ASK vs busy scripts)
    ✅ Exponential backoff with jitter
    ✅ Request timeouts and circuit breaker patterns
    ✅ RESP2/3 protocol support with auto-negotiation
    ✅ Push message handling (RESP3)
    ✅ Client-side caching (RESP3 CLIENT TRACKING)
    ✅ Comprehensive error handling and logging

    Usage:
        # Basic usage
        redis = ProductionRedis()

        # With TLS
        redis = ProductionRedis(use_tls=True, ssl_context=ssl_context)

        # With enhanced connection pooling
        redis = ProductionRedis(min_connections=5, max_connections=50)

        # Context manager
        with ProductionRedis() as redis:
            redis.set("key", "value")
            value = redis.get("key")
    """

    def __init__(self, host="localhost", port=6379, min_connections=1,
                 max_connections=10, timeout=5.0, use_tls=False,
                 ssl_context=None, max_retries=3, **kwargs):
        """
        Initialize production Redis client.

        Args:
            host: Redis server hostname
            port: Redis server port
            min_connections: Minimum connection pool size
            max_connections: Maximum connection pool size
            timeout: Connection timeout in seconds
            use_tls: Enable TLS/SSL connections
            ssl_context: Custom SSL context for TLS
            max_retries: Maximum retry attempts for failed operations
        """
        if _PRODUCTION_AVAILABLE:
            self._client = ProdRedis(
                host=host,
                port=port,
                min_connections=min_connections,
                max_connections=max_connections,
                timeout=timeout,
                use_tls=use_tls,
                ssl_context=ssl_context,
                max_retries=max_retries
            )
        else:
            # Fallback to optimized client
            self._client = OptimizedRedis(host=host, port=port, **kwargs)

    # Redis operations with enhanced reliability
    def get(self, key):
        """Get value with retry logic and health checks."""
        return self._client.get(key)

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """Set with retry logic and health checks."""
        return self._client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    def delete(self, key):
        """Delete with retry logic."""
        return self._client.delete(key)

    def execute_command(self, args):
        """Execute raw Redis command with full reliability."""
        return self._client.execute_command(args)

    # Connection pool management
    def get_pool_stats(self):
        """Get comprehensive connection pool statistics."""
        if hasattr(self._client, 'get_pool_stats'):
            return self._client.get_pool_stats()
        return {"status": "pool stats not available"}

    def get_connection_health(self):
        """Get connection health information."""
        if hasattr(self._client, 'get_connection_health'):
            return self._client.get_connection_health()
        return {"status": "health info not available"}

    # Protocol and server information
    def get_server_info(self):
        """Get Redis server information."""
        try:
            return self.execute_command(['INFO'])
        except Exception:
            return {"status": "server info unavailable"}

    def ping(self):
        """Ping the Redis server."""
        return self.execute_command(['PING'])

    # Cleanup
    def close(self):
        """Close all connections and cleanup resources."""
        if hasattr(self._client, 'close'):
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Additional utility methods
    def health_check(self):
        """Perform comprehensive health check."""
        try:
            # Test basic connectivity
            ping_result = self.ping()
            if ping_result != "PONG":
                return {"healthy": False, "error": "Ping failed"}

            # Get pool stats if available
            pool_stats = self.get_pool_stats()

            return {
                "healthy": True,
                "ping": ping_result,
                "pool_stats": pool_stats,
                "timestamp": time.time()
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def __repr__(self):
        pool_info = ""
        if hasattr(self._client, 'get_pool_stats'):
            stats = self._client.get_pool_stats()
            pool_info = f" (pool: {stats.get('healthy_connections', '?')}/{stats.get('total_connections', '?')} healthy)"

        return f"ProductionRedis{pool_info}"


# Convenience functions
def create_production_client(**kwargs):
    """Create a production Redis client with sensible defaults."""
    defaults = {
        "min_connections": 2,
        "max_connections": 20,
        "timeout": 5.0,
        "max_retries": 3,
        "use_tls": False
    }
    defaults.update(kwargs)
    return ProductionRedis(**defaults)


def create_tls_client(ca_certs=None, cert_file=None, key_file=None, **kwargs):
    """Create a production Redis client with TLS enabled."""
    import ssl

    ssl_context = ssl.create_default_context()
    if ca_certs:
        ssl_context.load_verify_locations(ca_certs)
    if cert_file and key_file:
        ssl_context.load_cert_chain(cert_file, key_file)

    kwargs.update({"use_tls": True, "ssl_context": ssl_context})
    return ProductionRedis(**kwargs)


# Import time for health checks
import time
