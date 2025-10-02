# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
Advanced CyRedis Features - SIMD Operations, Compression, and Performance Optimizations
"""

import time
import json
import zlib
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

# Import our optimized Redis client
from cy_redis.cy_redis_client import CyRedisClient

# Compression support
cdef extern from "zlib.h":
    int Z_DEFAULT_COMPRESSION
    int Z_BEST_SPEED
    int Z_BEST_COMPRESSION

# SIMD-like operations for bulk operations (conceptual - would need SIMD intrinsics for real SIMD)
cdef class CyBulkOperations:
    """
    High-performance bulk operations with SIMD-like processing.
    """

    cdef CyRedisClient redis
    cdef ThreadPoolExecutor executor
    cdef int batch_size
    cdef bint use_compression

    def __cinit__(self, CyRedisClient redis_client, int batch_size=100, bint use_compression=False):
        self.redis = redis_client
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.batch_size = batch_size
        self.use_compression = use_compression

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef list mget_bulk(self, list keys):
        """
        Bulk MGET with optimized pipelining.
        """
        cdef list results = []
        cdef int i = 0

        while i < len(keys):
            cdef int end_idx = min(i + self.batch_size, len(keys))
            cdef list batch_keys = keys[i:end_idx]

            # Use Redis MGET for bulk retrieval
            cdef str mget_cmd = "MGET"
            for key in batch_keys:
                mget_cmd += f" {key}"

            cdef list batch_results = self.redis.execute_command(mget_cmd.split())
            results.extend(batch_results)
            i = end_idx

        return results

    cpdef void mset_bulk(self, dict data):
        """
        Bulk MSET with optimized pipelining.
        """
        cdef list items = list(data.items())
        cdef int i = 0

        while i < len(items):
            cdef int end_idx = min(i + self.batch_size, len(items))
            cdef list batch_items = items[i:end_idx]

            # Use Redis MSET for bulk setting
            cdef list mset_args = ["MSET"]
            for key, value in batch_items:
                mset_args.extend([key, value])

            self.redis.execute_command(mset_args)
            i = end_idx

    cpdef list pipeline_execute(self, list operations):
        """
        Execute multiple operations in a pipeline for maximum throughput.
        """
        cdef list results = []

        for op in operations:
            if op[0] == "set":
                results.append(self.redis.set(op[1], op[2]))
            elif op[0] == "get":
                results.append(self.redis.get(op[1]))
            elif op[0] == "delete":
                results.append(self.redis.delete(op[1]))
            elif op[0] == "mget":
                results.append(self.mget_bulk(op[1]))
            elif op[0] == "mset":
                self.mset_bulk(op[1])
                results.append("OK")
            else:
                results.append(None)

        return results


# Compression utilities
cdef class CyCompression:
    """
    High-performance compression for large payloads.
    """

    cdef int level
    cdef bint enabled

    def __cinit__(self, int level=Z_DEFAULT_COMPRESSION, bint enabled=True):
        self.level = level
        self.enabled = enabled

    cpdef bytes compress(self, bytes data):
        """Compress data using zlib."""
        if not self.enabled or len(data) < 1024:  # Don't compress small data
            return data
        return zlib.compress(data, self.level)

    cpdef bytes decompress(self, bytes data):
        """Decompress data using zlib."""
        if not self.enabled:
            return data
        try:
            return zlib.decompress(data)
        except zlib.error:
            return data  # Return as-is if not compressed

    cpdef str compress_string(self, str data):
        """Compress string data."""
        cdef bytes data_bytes = data.encode('utf-8')
        cdef bytes compressed = self.compress(data_bytes)
        if compressed == data_bytes:
            return data  # Return original if compression didn't help
        return f"COMPRESSED:{compressed.hex()}"

    cpdef str decompress_string(self, str data):
        """Decompress string data."""
        if data.startswith("COMPRESSED:"):
            cdef str hex_data = data[11:]  # Remove "COMPRESSED:" prefix
            cdef bytes compressed = bytes.fromhex(hex_data)
            cdef bytes decompressed = self.decompress(compressed)
            return decompressed.decode('utf-8')
        return data


# Memory pool for frequently allocated objects
cdef class CyMemoryPool:
    """
    Memory pool for frequently allocated objects to reduce GC pressure.
    """

    cdef list pool
    cdef int max_size
    cdef object factory

    def __cinit__(self, object factory, int max_size=1000):
        self.pool = []
        self.max_size = max_size
        self.factory = factory

    cpdef object get(self):
        """Get an object from the pool or create new one."""
        if self.pool:
            return self.pool.pop()
        return self.factory()

    cpdef void put(self, object obj):
        """Return an object to the pool."""
        if len(self.pool) < self.max_size:
            self.pool.append(obj)


# Advanced metrics and monitoring
cdef class CyMetricsCollector:
    """
    High-performance metrics collection for monitoring and observability.
    """

    cdef dict counters
    cdef dict histograms
    cdef dict gauges
    cdef long start_time

    def __cinit__(self):
        self.counters = {}
        self.histograms = {}
        self.gauges = {}
        self.start_time = <long>time.time()

    cpdef void increment_counter(self, str name, long value=1):
        """Increment a counter metric."""
        if name not in self.counters:
            self.counters[name] = 0
        self.counters[name] += value

    cpdef void record_histogram(self, str name, double value):
        """Record a histogram value."""
        if name not in self.histograms:
            self.histograms[name] = []
        self.histograms[name].append((time.time(), value))

        # Keep only last 1000 values
        if len(self.histograms[name]) > 1000:
            self.histograms[name] = self.histograms[name][-1000:]

    cpdef void set_gauge(self, str name, double value):
        """Set a gauge value."""
        self.gauges[name] = value

    cpdef dict get_metrics(self):
        """Get all current metrics."""
        cdef dict result = {
            'counters': self.counters.copy(),
            'gauges': self.gauges.copy(),
            'histograms': {},
            'uptime': <long>time.time() - self.start_time
        }

        # Summarize histograms
        for name, values in self.histograms.items():
            if values:
                cdef list times = [v[0] for v in values]
                cdef list vals = [v[1] for v in values]
                result['histograms'][name] = {
                    'count': len(vals),
                    'min': min(vals),
                    'max': max(vals),
                    'avg': sum(vals) / len(vals),
                    'last_updated': max(times)
                }

        return result


# Circuit breaker pattern
cdef class CyCircuitBreaker:
    """
    Circuit breaker pattern for fault tolerance.
    """

    cdef str name
    cdef int failure_threshold
    cdef double recovery_timeout
    cdef int consecutive_failures
    cdef double last_failure_time
    cdef bint is_open

    def __cinit__(self, str name, int failure_threshold=5, double recovery_timeout=60.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.consecutive_failures = 0
        self.last_failure_time = 0
        self.is_open = False

    cpdef bint allow_request(self):
        """Check if request should be allowed."""
        if self.is_open:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                # Try to close the circuit
                self.is_open = False
                self.consecutive_failures = 0
                return True
            return False
        return True

    cpdef void record_success(self):
        """Record a successful operation."""
        self.consecutive_failures = 0

    cpdef void record_failure(self):
        """Record a failed operation."""
        self.consecutive_failures += 1
        self.last_failure_time = time.time()

        if self.consecutive_failures >= self.failure_threshold:
            self.is_open = True

    cpdef dict get_state(self):
        """Get circuit breaker state."""
        return {
            'name': self.name,
            'is_open': self.is_open,
            'consecutive_failures': self.consecutive_failures,
            'last_failure_time': self.last_failure_time,
            'failure_threshold': self.failure_threshold,
            'recovery_timeout': self.recovery_timeout
        }


# Advanced Redis client with all optimizations
cdef class CyAdvancedRedisClient:
    """
    Advanced Redis client with compression, bulk operations, metrics, and circuit breakers.
    """

    cdef CyRedisClient redis
    cdef CyBulkOperations bulk_ops
    cdef CyCompression compression
    cdef CyMemoryPool memory_pool
    cdef CyMetricsCollector metrics
    cdef CyCircuitBreaker circuit_breaker
    cdef ThreadPoolExecutor executor

    def __cinit__(self, CyRedisClient redis_client):
        self.redis = redis_client
        self.bulk_ops = CyBulkOperations(redis_client, batch_size=100, use_compression=True)
        self.compression = CyCompression(level=Z_BEST_SPEED, enabled=True)
        self.memory_pool = CyMemoryPool(lambda: {}, max_size=1000)
        self.metrics = CyMetricsCollector()
        self.circuit_breaker = CyCircuitBreaker("redis_client", failure_threshold=5, recovery_timeout=30.0)
        self.executor = ThreadPoolExecutor(max_workers=8)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    # Advanced operations with all optimizations
    cpdef object get(self, str key):
        """Get with metrics and circuit breaker."""
        cdef long start_time = <long>time.time()

        if not self.circuit_breaker.allow_request():
            self.metrics.increment_counter("circuit_breaker_open")
            raise Exception("Circuit breaker is open")

        try:
            cdef object result = self.redis.get(key)
            cdef long duration = <long>time.time() - start_time

            self.metrics.record_histogram("get_duration", duration)
            self.metrics.increment_counter("get_success")
            self.circuit_breaker.record_success()

            return result
        except Exception as e:
            self.metrics.increment_counter("get_failure")
            self.circuit_breaker.record_failure()
            raise e

    cpdef object set(self, str key, str value):
        """Set with compression and metrics."""
        cdef long start_time = <long>time.time()

        if not self.circuit_breaker.allow_request():
            self.metrics.increment_counter("circuit_breaker_open")
            raise Exception("Circuit breaker is open")

        try:
            # Compress if beneficial
            cdef str compressed_value = self.compression.compress_string(value)

            cdef object result = self.redis.set(key, compressed_value)
            cdef long duration = <long>time.time() - start_time

            self.metrics.record_histogram("set_duration", duration)
            self.metrics.increment_counter("set_success")
            self.circuit_breaker.record_success()

            return result
        except Exception as e:
            self.metrics.increment_counter("set_failure")
            self.circuit_breaker.record_failure()
            raise e

    cpdef list mget(self, list keys):
        """Bulk get with optimizations."""
        cdef long start_time = <long>time.time()

        if not self.circuit_breaker.allow_request():
            self.metrics.increment_counter("circuit_breaker_open")
            raise Exception("Circuit breaker is open")

        try:
            cdef list results = self.bulk_ops.mget_bulk(keys)
            cdef long duration = <long>time.time() - start_time

            self.metrics.record_histogram("mget_duration", duration)
            self.metrics.increment_counter("mget_success")
            self.circuit_breaker.record_success()

            return results
        except Exception as e:
            self.metrics.increment_counter("mget_failure")
            self.circuit_breaker.record_failure()
            raise e

    cpdef dict get_metrics(self):
        """Get comprehensive metrics."""
        return self.metrics.get_metrics()

    cpdef dict get_circuit_breaker_state(self):
        """Get circuit breaker state."""
        return self.circuit_breaker.get_state()

    # Async versions
    async def get_async(self, str key):
        """Async get with full optimizations."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.get, key)

    async def set_async(self, str key, str value):
        """Async set with full optimizations."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.set, key, value)

    async def mget_async(self, list keys):
        """Async bulk get."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.mget, keys)


# Import asyncio for async methods
import asyncio
