# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
High-Performance Cython RPC (Remote Procedure Call) Layer
Optimized distributed RPC capabilities over Redis with service discovery and load balancing.
"""

import json
import time
import uuid
from typing import Dict, List, Optional, Any, Callable, Union
from concurrent.futures import ThreadPoolExecutor, Future
import threading

# Import our optimized Redis client
from cy_redis.cy_redis_client import CyRedisClient


# Exception classes (keep as Python classes for compatibility)
class RPCError(Exception):
    """Base exception for RPC operations"""
    pass

class RPCServiceUnavailable(RPCError):
    """Raised when no service instances are available"""
    pass

class RPCTimeoutError(RPCError):
    """Raised when RPC call times out"""
    pass


# Optimized RPC Request
cdef class CyRPCRequest:
    """
    High-performance RPC request with optimized serialization.
    """

    def __cinit__(self, str service, str method, list args=None,
                  dict kwargs=None, str request_id=None, int timeout=30):
        self.service = service
        self.method = method
        self.args = args or []
        self.kwargs = kwargs or {}
        self.request_id = request_id or str(uuid.uuid4())
        self.timestamp = <long>time.time()
        self.timeout = timeout

    def to_dict(self):
        """Convert request to dictionary for serialization"""
        return {
            'service': self.service,
            'method': self.method,
            'args': self.args,
            'kwargs': self.kwargs,
            'request_id': self.request_id,
            'timestamp': self.timestamp,
            'timeout': self.timeout
        }

    @staticmethod
    def from_dict(dict data):
        """Create request from dictionary"""
        return CyRPCRequest(
            data['service'],
            data['method'],
            data.get('args', []),
            data.get('kwargs', {}),
            data.get('request_id'),
            data.get('timeout', 30)
        )

    def to_json(self):
        """Serialize to JSON"""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(str json_str):
        """Deserialize from JSON"""
        return CyRPCRequest.from_dict(json.loads(json_str))


# Optimized RPC Response
cdef class CyRPCResponse:
    """
    High-performance RPC response with optimized serialization.
    """

    def __cinit__(self, str request_id, bint success=True, result=None,
                  str error=None, str error_type=None):
        self.request_id = request_id
        self.success = success
        self.result = result
        self.error = error
        self.error_type = error_type
        self.timestamp = <long>time.time()

    def to_dict(self):
        """Convert response to dictionary for serialization"""
        return {
            'request_id': self.request_id,
            'success': self.success,
            'result': self.result,
            'error': self.error,
            'error_type': self.error_type,
            'timestamp': self.timestamp
        }

    @staticmethod
    def from_dict(dict data):
        """Create response from dictionary"""
        return CyRPCResponse(
            data['request_id'],
            data.get('success', True),
            data.get('result'),
            data.get('error'),
            data.get('error_type')
        )

    def to_json(self):
        """Serialize to JSON"""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(str json_str):
        """Deserialize from JSON"""
        return CyRPCResponse.from_dict(json.loads(json_str))


# Optimized Service Registry
cdef class CyRPCServiceRegistry:
    """
    High-performance service registry with optimized service discovery.
    """

    def __cinit__(self, redis_client, str namespace="rpc",
                  int heartbeat_interval=30):
        self.redis = redis_client
        self.registry_key = f"{namespace}:services"
        self.heartbeat_key = f"{namespace}:heartbeats"
        self.heartbeat_interval = heartbeat_interval
        self.executor = ThreadPoolExecutor(max_workers=2)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef void register_service(self, str service_name, str service_id,
                               dict metadata=None, str endpoint=""):
        """
        Register a service instance with optimized Redis operations.
        """
        cdef str service_key = f"{self.registry_key}:{service_name}:{service_id}"
        cdef dict service_info = {
            'service_id': service_id,
            'endpoint': endpoint,
            'metadata': metadata or {},
            'registered_at': <long>time.time(),
            'last_heartbeat': <long>time.time()
        }

        # Store service info
        self.redis.set(service_key, json.dumps(service_info))

        # Add to service set
        cdef str service_set_key = f"{self.registry_key}:{service_name}"
        self.redis.execute_command(['SADD', service_set_key, service_id])

        # Set expiration
        self.redis.execute_command(['EXPIRE', service_key, str(self.heartbeat_interval * 3)])

    cpdef void unregister_service(self, str service_name, str service_id):
        """
        Unregister a service instance.
        """
        cdef str service_key = f"{self.registry_key}:{service_name}:{service_id}"
        cdef str service_set_key = f"{self.registry_key}:{service_name}"

        # Remove from service set
        self.redis.execute_command(['SREM', service_set_key, service_id])

        # Delete service info
        self.redis.delete(service_key)

    cpdef list discover_services(self, str service_name):
        """
        Discover available service instances with optimized lookup.
        """
        cdef str service_set_key = f"{self.registry_key}:{service_name}"
        cdef list service_ids = self.redis.execute_command(['SMEMBERS', service_set_key]) or []
        cdef list services = []
        cdef str service_id, service_key, service_json
        cdef dict service_info
        cdef long now, last_heartbeat

        for service_id in service_ids:
            service_key = f"{self.registry_key}:{service_name}:{service_id}"
            service_json = self.redis.get(service_key)

            if service_json:
                try:
                    service_info = json.loads(service_json)
                    # Check if service is still alive (recent heartbeat)
                    now = <long>time.time()
                    last_heartbeat = service_info.get('last_heartbeat', 0)

                    if now - last_heartbeat <= self.heartbeat_interval * 2:
                        services.append(service_info)
                    else:
                        # Service appears dead, remove it
                        self.redis.execute_command(['SREM', service_set_key, service_id])
                        self.redis.delete(service_key)
                except (json.JSONDecodeError, KeyError):
                    # Invalid service data, clean up
                    self.redis.execute_command(['SREM', service_set_key, service_id])
                    self.redis.delete(service_key)

        return services

    cpdef void send_heartbeat(self, str service_name, str service_id):
        """
        Send heartbeat for service instance.
        """
        cdef str service_key = f"{self.registry_key}:{service_name}:{service_id}"
        cdef str service_json = self.redis.get(service_key)
        cdef dict service_info

        if service_json:
            try:
                service_info = json.loads(service_json)
                service_info['last_heartbeat'] = <long>time.time()
                self.redis.set(service_key, json.dumps(service_info))
                self.redis.execute_command(['EXPIRE', service_key, str(self.heartbeat_interval * 3)])
            except (json.JSONDecodeError, KeyError):
                pass  # Service data corrupted, will be cleaned up on discovery

    cpdef list get_all_services(self):
        """
        Get all registered services.
        """
        cdef list services = []
        cdef str pattern = f"{self.registry_key}:*:*"
        cdef str cursor = "0"
        cdef list scan_result, keys
        cdef str key, service_json

        # Use SCAN to find all service keys
        while cursor != "0" or not services:
            scan_result = self.redis.execute_command(['SCAN', cursor, 'MATCH', pattern])
            if not scan_result or len(scan_result) < 2:
                break

            cursor = scan_result[0]
            keys = scan_result[1]

            for key in keys:
                service_json = self.redis.get(key)
                if service_json:
                    try:
                        services.append(json.loads(service_json))
                    except json.JSONDecodeError:
                        pass

            if cursor == "0":
                break

        return services


# Optimized RPC Client
cdef class CyRPCClient:
    """
    High-performance RPC client with optimized request/response handling.
    """

    def __cinit__(self, redis_client, str namespace="rpc", int default_timeout=30):
        self.redis = redis_client
        self.registry = CyRPCServiceRegistry(redis_client, namespace)
        self.request_queue_prefix = f"{namespace}:requests"
        self.response_prefix = f"{namespace}:responses"
        self.default_timeout = default_timeout
        self.executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self.executor:
            self.executor.shutdown(wait=True)

    cpdef object call(self, str service_name, str method, list args=None,
                     dict kwargs=None, int timeout=-1):
        """
        Make synchronous RPC call with optimized performance.
        """
        if timeout == -1:
            timeout = self.default_timeout

        # Variable declarations
        cdef CyRPCRequest request
        cdef list services
        cdef dict service
        cdef str service_id, request_key, response_key, response_json
        cdef long start_time
        cdef CyRPCResponse response

        # Create request
        request = CyRPCRequest(service_name, method, args, kwargs, timeout=timeout)

        # Discover service instances
        services = self.registry.discover_services(service_name)
        if not services:
            raise RPCServiceUnavailable(f"No instances available for service: {service_name}")

        # Simple load balancing - pick first available service
        service = services[0]
        service_id = service['service_id']

        # Send request
        request_key = f"{self.request_queue_prefix}:{service_id}"
        self.redis.execute_command(['LPUSH', request_key, request.to_json()])

        # Wait for response
        response_key = f"{self.response_prefix}:{request.request_id}"
        start_time = <long>time.time()

        while (<long>time.time() - start_time) < timeout:
            response_json = self.redis.get(response_key)
            if response_json:
                # Clean up response
                self.redis.delete(response_key)

                response = CyRPCResponse.from_json(response_json)
                if response.success:
                    return response.result
                else:
                    # Raise appropriate exception
                    if response.error_type == "RPCServiceUnavailable":
                        raise RPCServiceUnavailable(response.error)
                    elif response.error_type == "RPCTimeoutError":
                        raise RPCTimeoutError(response.error)
                    else:
                        raise RPCError(response.error or "RPC call failed")

            time.sleep(0.01)  # Small delay to avoid busy waiting

        raise RPCTimeoutError(f"RPC call to {service_name}.{method} timed out")

    async def call_async(self, str service_name, str method, list args=None,
                        dict kwargs=None, int timeout=-1):
        """Async RPC call"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor, self.call, service_name, method, args, kwargs, timeout
        )


# Additional RPC classes will be implemented...
# CyRPCServer, CyRPCService, CyRedisPluginManager

# Import asyncio for async methods
import asyncio
