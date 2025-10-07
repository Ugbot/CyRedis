# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Advanced Features
"""

# Bulk Operations
cdef class CyBulkOperations:
    cdef object redis
    cdef object executor
    cdef int batch_size
    cdef bint use_compression

# Compression
cdef class CyCompression:
    cdef int level
    cdef bint enabled

# Memory Pool
cdef class CyMemoryPool:
    cdef list pool
    cdef int max_size
    cdef object factory

# Metrics
cdef class CyMetricsCollector:
    cdef dict counters
    cdef dict histograms
    cdef dict gauges
    cdef long start_time

# Circuit Breaker
cdef class CyCircuitBreaker:
    cdef str name
    cdef int failure_threshold
    cdef double recovery_timeout
    cdef int consecutive_failures
    cdef double last_failure_time
    cdef bint is_open

# Advanced Client
cdef class CyAdvancedRedisClient:
    cdef object redis
    cdef CyBulkOperations bulk_ops
    cdef CyCompression compression
    cdef CyMemoryPool memory_pool
    cdef CyMetricsCollector metrics
    cdef CyCircuitBreaker circuit_breaker
    cdef object executor
