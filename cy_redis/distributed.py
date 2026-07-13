"""
Alias module for distributed primitives, matching test expectations.
"""

from cy_redis.features.distributed import CyDistributedLock, CyReadWriteLock

__all__ = ["CyDistributedLock", "CyReadWriteLock"]
