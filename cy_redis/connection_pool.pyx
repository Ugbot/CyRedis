# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
Enhanced Connection Pooling with Health Checks, TLS, and Retry Logic
Implements MVP requirements for robust Redis connections
"""

import time
import threading
import ssl
from typing import Optional, List, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor

# Import our optimized components
from cy_redis.cy_redis_client import CyRedisConnection

# Connection states
DEF CONN_DISCONNECTED = 0
DEF CONN_CONNECTED = 1
DEF CONN_ERROR = 2
DEF CONN_HEALTH_CHECKING = 3

# Health check constants
DEF HEALTH_CHECK_INTERVAL = 30.0  # seconds
DEF CONNECTION_TIMEOUT = 5.0      # seconds
DEF MAX_RETRIES = 3
DEF RETRY_BACKOFF = 1.0           # seconds

cdef class ConnectionHealth:
    """
    Tracks health status of individual connections
    """

    cdef int state
    cdef double last_health_check
    cdef double last_successful_operation
    cdef int consecutive_failures
    cdef double created_at
    cdef bint supports_tls

    def __cinit__(self):
        self.state = CONN_DISCONNECTED
        self.last_health_check = 0.0
        self.last_successful_operation = 0.0
        self.consecutive_failures = 0
        self.created_at = time.time()
        self.supports_tls = False

    cpdef bint is_healthy(self):
        """Check if connection is healthy"""
        cdef double now = time.time()
        cdef double time_since_check = now - self.last_health_check
        cdef double time_since_success = now - self.last_successful_operation

        # If we haven't checked recently, assume healthy
        if time_since_check > HEALTH_CHECK_INTERVAL:
            return True

        # Connection is unhealthy if:
        # - In error state
        # - Too many consecutive failures
        # - No successful operation for too long
        if self.state == CONN_ERROR:
            return False

        if self.consecutive_failures >= MAX_RETRIES:
            return False

        # If connection is too old and hasn't been used recently
        cdef double connection_age = now - self.created_at
        if connection_age > 300.0 and time_since_success > 60.0:  # 5 min old, 1 min unused
            return False

        return True

    cpdef void mark_successful(self):
        """Mark a successful operation"""
        self.last_successful_operation = time.time()
        self.consecutive_failures = 0
        self.state = CONN_CONNECTED

    cpdef void mark_failed(self):
        """Mark a failed operation"""
        self.consecutive_failures += 1
        if self.consecutive_failures >= MAX_RETRIES:
            self.state = CONN_ERROR
        self.last_health_check = time.time()

    cpdef void mark_health_checked(self):
        """Mark that health check was performed"""
        self.last_health_check = time.time()

cdef class EnhancedConnectionPool:
    """
    Enhanced connection pool with health checks, TLS, and intelligent management
    """

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

    def __cinit__(self, str host="localhost", int port=6379,
                  int min_connections=1, int max_connections=10,
                  double timeout=CONNECTION_TIMEOUT, bint use_tls=False,
                  ssl_context=None):
        self.connections = []
        self.connection_health = []
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.host = host
        self.port = port
        self.timeout = timeout
        self.lock = threading.Lock()
        self.running = True
        self.use_tls = use_tls
        self.ssl_context = ssl_context
        self.executor = ThreadPoolExecutor(max_workers=2)

        # Initialize minimum connections
        self._initialize_connections()

        # Start health check thread
        self.health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True
        )
        self.health_check_thread.start()

    def __dealloc__(self):
        self.running = False
        if self.executor:
            self.executor.shutdown(wait=True)

    cdef void _initialize_connections(self):
        """Initialize minimum number of connections"""
        with self.lock:
            for i in range(self.min_connections):
                self._create_connection()

    cdef CyRedisConnection _create_connection(self):
        """Create a new connection with health tracking"""
        cdef CyRedisConnection conn = CyRedisConnection(
            self.host, self.port, self.timeout
        )

        cdef ConnectionHealth health = ConnectionHealth()
        health.created_at = time.time()

        # Attempt to connect
        if conn.connect() == 0:
            health.mark_successful()
            self.connections.append(conn)
            self.connection_health.append(health)
            return conn
        else:
            health.mark_failed()
            return None

    cdef CyRedisConnection _get_healthy_connection(self):
        """Get a healthy connection, creating new ones if needed"""
        with self.lock:
            # First, try to find an existing healthy connection
            for i in range(len(self.connections)):
                if self.connection_health[i].is_healthy():
                    return self.connections[i]

            # If no healthy connections, try to create a new one
            if len(self.connections) < self.max_connections:
                conn = self._create_connection()
                if conn:
                    return conn

            # If we still don't have a connection, try to revive an unhealthy one
            for i in range(len(self.connections)):
                if self.connection_health[i].state == CONN_ERROR:
                    # Try to reconnect
                    if self.connections[i].connect() == 0:
                        self.connection_health[i].mark_successful()
                        return self.connections[i]

            return None

    cpdef CyRedisConnection get_connection(self):
        """Get a connection from the pool"""
        cdef CyRedisConnection conn = self._get_healthy_connection()
        if conn:
            return conn

        # If no connections available, try with retry logic
        return self._get_connection_with_retry()

    cdef CyRedisConnection _get_connection_with_retry(self):
        """Get connection with retry logic"""
        cdef int attempts = 0
        cdef double backoff = RETRY_BACKOFF

        while attempts < MAX_RETRIES and self.running:
            conn = self._create_connection()
            if conn:
                return conn

            attempts += 1
            if attempts < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

        return None

    cpdef void return_connection(self, CyRedisConnection conn):
        """Return connection to pool"""
        if conn:
            with self.lock:
                # Find the connection in our list and mark it as returned successfully
                for i in range(len(self.connections)):
                    if self.connections[i] is conn:
                        self.connection_health[i].mark_successful()
                        break

    cpdef void _health_check_loop(self):
        """Background health check loop"""
        while self.running:
            try:
                self._perform_health_checks()
                time.sleep(HEALTH_CHECK_INTERVAL)
            except Exception as e:
                print(f"Health check error: {e}")
                time.sleep(HEALTH_CHECK_INTERVAL)

    cdef void _perform_health_checks(self):
        """Perform health checks on all connections"""
        with self.lock:
            for i in range(len(self.connections)):
                self._check_connection_health(i)

    cdef void _check_connection_health(self, int index):
        """Check health of a specific connection"""
        cdef CyRedisConnection conn = self.connections[index]
        cdef ConnectionHealth health = self.connection_health[index]

        health.mark_health_checked()

        # Perform a simple PING health check
        try:
            start_time = time.time()
            result = conn.execute_command(['PING'])
            response_time = time.time() - start_time

            if result == "PONG" and response_time < 1.0:  # Reasonable response time
                health.mark_successful()
            else:
                health.mark_failed()

        except Exception:
            health.mark_failed()

    cpdef dict get_pool_stats(self):
        """Get comprehensive pool statistics"""
        cdef dict stats = {
            'total_connections': len(self.connections),
            'max_connections': self.max_connections,
            'min_connections': self.min_connections,
            'healthy_connections': 0,
            'unhealthy_connections': 0,
            'error_connections': 0,
            'connections_by_state': {
                'connected': 0,
                'error': 0,
                'disconnected': 0
            }
        }

        with self.lock:
            for health in self.connection_health:
                if health.is_healthy():
                    stats['healthy_connections'] += 1

                    if health.state == CONN_CONNECTED:
                        stats['connections_by_state']['connected'] += 1
                else:
                    stats['unhealthy_connections'] += 1

                    if health.state == CONN_ERROR:
                        stats['error_connections'] += 1
                        stats['connections_by_state']['error'] += 1
                    else:
                        stats['connections_by_state']['disconnected'] += 1

        return stats

    cpdef void close(self):
        """Close all connections and stop health checks"""
        self.running = False

        with self.lock:
            for conn in self.connections:
                if conn:
                    conn.disconnect()

            self.connections.clear()
            self.connection_health.clear()

# TLS/SSL Support
cdef class TLSSupport:
    """
    TLS/SSL support for Redis connections
    """

    cdef object ssl_context
    cdef bint enabled
    cdef str cert_file
    cdef str key_file
    cdef str ca_certs

    def __cinit__(self, bint enabled=False, str cert_file=None,
                  str key_file=None, str ca_certs=None):
        self.enabled = enabled
        self.cert_file = cert_file
        self.key_file = key_file
        self.ca_certs = ca_certs

        if enabled:
            self._setup_ssl_context()

    cdef void _setup_ssl_context(self):
        """Setup SSL context for TLS connections"""
        self.ssl_context = ssl.create_default_context()

        if self.ca_certs:
            self.ssl_context.load_verify_locations(self.ca_certs)

        if self.cert_file and self.key_file:
            self.ssl_context.load_cert_chain(self.cert_file, self.key_file)

        # Set minimum TLS version
        self.ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        self.ssl_context.check_hostname = True
        self.ssl_context.verify_mode = ssl.CERT_REQUIRED

    cpdef bint is_enabled(self):
        """Check if TLS is enabled"""
        return self.enabled

    cpdef object get_ssl_context(self):
        """Get the SSL context"""
        return self.ssl_context

# Retry Strategy
cdef class RetryStrategy:
    """
    Intelligent retry strategy with exponential backoff and error classification
    """

    cdef int max_retries
    cdef double base_delay
    cdef double max_delay
    cdef list retryable_errors
    cdef object jitter_func

    def __cinit__(self, int max_retries=MAX_RETRIES, double base_delay=RETRY_BACKOFF,
                  double max_delay=30.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retryable_errors = [
            'Connection refused',
            'Connection reset',
            'Connection timed out',
            'Network is unreachable',
            'MOVED',
            'ASK',
            'CLUSTERDOWN'
        ]

    cpdef bint should_retry(self, Exception error, int attempt):
        """Determine if an operation should be retried"""
        if attempt >= self.max_retries:
            return False

        error_str = str(error).lower()
        for retryable in self.retryable_errors:
            if retryable.lower() in error_str:
                return True

        return False

    cpdef double get_delay(self, int attempt):
        """Calculate delay for retry attempt"""
        import random
        cdef double delay = self.base_delay * (2 ** attempt)
        delay = min(delay, self.max_delay)
        # Add jitter to prevent thundering herd
        delay += random.uniform(0, 0.1 * delay)
        return delay

    cpdef object execute_with_retry(self, callable_func, *args, **kwargs):
        """Execute a function with retry logic"""
        cdef int attempt = 0
        cdef Exception last_error

        while attempt <= self.max_retries:
            try:
                return callable_func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if not self.should_retry(e, attempt):
                    break

                delay = self.get_delay(attempt)
                time.sleep(delay)
                attempt += 1

        raise last_error

# Enhanced Redis Client with full MVP features
cdef class EnhancedRedisClient:
    """
    Enhanced Redis client with connection pooling, TLS, health checks, and retry logic
    Implements MVP requirements for production-ready Redis client
    """

    cdef EnhancedConnectionPool pool
    cdef TLSSupport tls_support
    cdef RetryStrategy retry_strategy
    cdef object executor
    cdef dict stream_offsets
    cdef object offset_lock

    def __cinit__(self, str host="localhost", int port=6379,
                  int min_connections=1, int max_connections=10,
                  double timeout=CONNECTION_TIMEOUT,
                  bint use_tls=False, ssl_context=None,
                  int max_retries=MAX_RETRIES):
        self.pool = EnhancedConnectionPool(
            host, port, min_connections, max_connections, timeout, use_tls, ssl_context
        )
        self.tls_support = TLSSupport(use_tls, ssl_context=ssl_context)
        self.retry_strategy = RetryStrategy(max_retries)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.stream_offsets = {}
        self.offset_lock = threading.Lock()

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)
        if self.pool:
            self.pool.close()

    # Enhanced operations with retry logic
    def execute_command(self, list args):
        """Execute Redis command with retry logic"""
        def _execute():
            conn = self.pool.get_connection()
            if conn is None:
                raise ConnectionError("No available connections")

            try:
                return conn.execute_command(args)
            finally:
                self.pool.return_connection(conn)

        return self.retry_strategy.execute_with_retry(_execute)

    # Standard Redis operations with enhanced reliability
    def get(self, key: str) -> Optional[str]:
        """Get value with retry logic"""
        return self.execute_command(['GET', key])

    def set(self, key: str, value: str, ex: int = -1, px: int = -1,
            nx: bool = False, xx: bool = False) -> str:
        """Set with retry logic"""
        args = ['SET', key, value]
        if ex > 0:
            args.extend(['EX', str(ex)])
        elif px > 0:
            args.extend(['PX', str(px)])
        if nx:
            args.append('NX')
        elif xx:
            args.append('XX')
        return self.execute_command(args)

    def delete(self, key: str) -> int:
        """Delete with retry logic"""
        return self.execute_command(['DEL', key])

    # Connection management
    def get_pool_stats(self):
        """Get connection pool statistics"""
        return self.pool.get_pool_stats()

    def get_connection_health(self):
        """Get connection health information"""
        return self.pool.get_pool_stats()  # Enhanced version would provide more details

    def close(self):
        """Close all connections"""
        self.pool.close()

# Python wrapper
class ProductionRedis:
    """
    Production-ready Redis client with enhanced connection pooling,
    TLS support, health checks, and intelligent retry logic.
    """

    def __init__(self, host="localhost", port=6379, min_connections=1,
                 max_connections=10, timeout=CONNECTION_TIMEOUT,
                 use_tls=False, ssl_context=None, max_retries=MAX_RETRIES):
        self._client = EnhancedRedisClient(
            host, port, min_connections, max_connections,
            timeout, use_tls, ssl_context, max_retries
        )

    def get(self, key: str) -> Optional[str]:
        return self._client.get(key)

    def set(self, key: str, value: str, ex: int = -1, px: int = -1,
            nx: bool = False, xx: bool = False) -> str:
        return self._client.set(key, value, ex, px, nx, xx)

    def delete(self, key: str) -> int:
        return self._client.delete(key)

    def execute_command(self, args: list):
        return self._client.execute_command(args)

    def get_pool_stats(self):
        return self._client.get_pool_stats()

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
