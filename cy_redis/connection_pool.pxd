# cython: language_level=3
# distutils: language=c

"""
Header declarations for Enhanced Connection Pooling
"""

from cy_redis.cy_redis_client cimport CyRedisConnection

# Connection states
DEF CONN_DISCONNECTED = 0
DEF CONN_CONNECTED = 1
DEF CONN_ERROR = 2
DEF CONN_HEALTH_CHECKING = 3

# Health check constants
DEF HEALTH_CHECK_INTERVAL = 30.0
DEF CONNECTION_TIMEOUT = 5.0
DEF MAX_RETRIES = 3
DEF RETRY_BACKOFF = 1.0

# Connection health tracker
cdef class ConnectionHealth:
    cdef int state
    cdef double last_health_check
    cdef double last_successful_operation
    cdef int consecutive_failures
    cdef double created_at
    cdef bint supports_tls

# Enhanced connection pool
cdef class EnhancedConnectionPool:
    cdef list connections
    cdef list connection_health
    cdef int max_connections
    cdef int min_connections
    cdef str host
    cdef int port
    cdef double timeout
    cdef object lock
    cdef object health_check_thread
    cdef bint running
    cdef bint use_tls
    cdef object ssl_context
    cdef object executor
    cdef object retry_strategy

# TLS support
cdef class TLSSupport:
    cdef object ssl_context
    cdef bint enabled
    cdef str cert_file
    cdef str key_file
    cdef str ca_certs

# Retry strategy
cdef class RetryStrategy:
    cdef int max_retries
    cdef double base_delay
    cdef double max_delay
    cdef list retryable_errors
    cdef object jitter_func

# Enhanced Redis client
cdef class EnhancedRedisClient:
    cdef EnhancedConnectionPool pool
    cdef TLSSupport tls_support
    cdef RetryStrategy retry_strategy
    cdef object executor
    cdef dict stream_offsets
    cdef object offset_lock
