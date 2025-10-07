# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis RPC Components
"""

# No forward declarations needed - using object types

# RPC Request
cdef class CyRPCRequest:
    cdef str service
    cdef str method
    cdef list args
    cdef dict kwargs
    cdef str request_id
    cdef long timestamp
    cdef int timeout

# RPC Response
cdef class CyRPCResponse:
    cdef str request_id
    cdef bint success
    cdef object result
    cdef str error
    cdef str error_type
    cdef long timestamp

# Service Registry
cdef class CyRPCServiceRegistry:
    cdef object redis  # CyRedisClient
    cdef str registry_key
    cdef str heartbeat_key
    cdef int heartbeat_interval
    cdef object executor

    cpdef void register_service(self, str service_name, str service_id, dict metadata=?, str endpoint=?)
    cpdef void unregister_service(self, str service_name, str service_id)
    cpdef list discover_services(self, str service_name)
    cpdef void send_heartbeat(self, str service_name, str service_id)
    cpdef list get_all_services(self)

# RPC Client
cdef class CyRPCClient:
    cdef object redis  # CyRedisClient
    cdef CyRPCServiceRegistry registry
    cdef str request_queue_prefix
    cdef str response_prefix
    cdef int default_timeout
    cdef object executor

    cpdef object call(self, str service_name, str method, list args=?, dict kwargs=?, int timeout=?)

# Additional classes will be declared here as they are implemented
