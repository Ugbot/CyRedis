"""
Integration Tests for CyRedis

Comprehensive integration tests for CyRedis library covering:
- Basic Redis operations (GET, SET, etc.)
- Concurrent access patterns
- Async operations
- Messaging workflows (pub/sub, queues)
- Distributed locking
- Connection resilience
- Cluster operations
- Sentinel failover
- Streaming operations
- Lua scripts
- Performance benchmarks
- PostgreSQL caching integration

Requirements:
- Redis server running (standalone, cluster, or sentinel as needed)
- PostgreSQL server (for pgcache tests)
- Docker (for some integration tests)

Use pytest markers to run specific test groups:
  pytest -m integration        # Run all integration tests
  pytest -m "not slow"         # Skip slow tests
  pytest -m cluster            # Run only cluster tests
  pytest -m requires_redis     # Run tests requiring Redis
  pytest -m pgcache            # Run PostgreSQL cache tests
"""

__all__ = []
