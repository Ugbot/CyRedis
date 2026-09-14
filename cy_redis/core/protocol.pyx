# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""RESP protocol version negotiation and per-connection capability state.

Reply parsing lives in ``cy_redis_client.pyx`` next to the hiredis calls that
produce the replies; this module only knows how to ask a server which
protocol it speaks and remember the answer.
"""

# Protocol version constants (module-level for Python import and default params)
RESP2 = 2
RESP3 = 3


class RedisProtocolError(Exception):
    """Protocol-level error"""
    pass


class RedisConnectionError(Exception):
    """Connection-related error"""
    pass


cdef class ProtocolNegotiator:
    """
    Handles RESP protocol version negotiation with Redis server
    """

    cdef object redis_client
    cdef int preferred_version

    def __cinit__(self, redis_client, int preferred_version=RESP3):
        self.redis_client = redis_client
        self.preferred_version = preferred_version

    cpdef int negotiate_protocol(self):
        """
        Negotiate protocol version with server using HELLO command
        Returns the negotiated protocol version
        """
        try:
            # Try RESP3 HELLO command
            result = self.redis_client.execute_command(['HELLO', '3'])
            if isinstance(result, dict) and 'proto' in result:
                return RESP3
        except Exception:
            # Fall back to RESP2
            pass

        return RESP2

    cpdef void set_server_protocol(self, int version):
        """Set server to use specific protocol version"""
        if version == RESP3:
            try:
                self.redis_client.execute_command(['HELLO', '3'])
            except Exception:
                # Server doesn't support RESP3
                pass
        elif version == RESP2:
            try:
                # Switch back to RESP2 if supported
                self.redis_client.execute_command(['HELLO', '2'])
            except Exception:
                # Server doesn't support HELLO or RESP2 switching
                pass


cdef class ConnectionState:
    """
    Tracks connection state including protocol version and capabilities
    """

    cdef public int protocol_version
    cdef public bint supports_resp3
    cdef bint supports_pipelining
    cdef bint supports_scripts
    cdef bint supports_pubsub
    cdef public dict server_info

    def __cinit__(self):
        self.protocol_version = RESP2
        self.supports_resp3 = False
        self.supports_pipelining = True  # Most Redis versions support this
        self.supports_scripts = True     # Most Redis versions support this
        self.supports_pubsub = True      # Most Redis versions support this
        self.server_info = {}

    cpdef void update_from_hello(self, dict hello_response):
        """Update state from HELLO command response"""
        # Precondition: HELLO is parsed into a RESP3 map (Python dict).
        assert hello_response is not None, "hello response must not be None"
        assert isinstance(hello_response, dict), "hello response must be a dict"
        if 'proto' in hello_response:
            self.protocol_version = hello_response['proto']
            self.supports_resp3 = self.protocol_version >= 3

        # Store server capabilities
        self.server_info = hello_response

    cpdef void update_from_info(self, str info_response):
        """Update state from INFO command response"""
        # Precondition: INFO returns a text blob to be parsed line by line.
        assert info_response is not None, "info response must not be None"
        assert isinstance(info_response, str), "info response must be a string"
        # Parse Redis INFO command output
        lines = info_response.split('\n')
        cdef dict section = {}
        cdef str current_section = ""

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                if section and current_section:
                    self.server_info[current_section] = section
                    section = {}
                current_section = line[1:].strip() if line.startswith('#') else ""
                continue

            if ':' in line:
                key, value = line.split(':', 1)
                section[key] = value

        if section and current_section:
            self.server_info[current_section] = section

    cpdef bint supports_feature(self, str feature):
        """Check if server supports a specific feature"""
        if feature == "resp3":
            return self.supports_resp3
        elif feature == "pipelining":
            return self.supports_pipelining
        elif feature == "scripts":
            return self.supports_scripts
        elif feature == "pubsub":
            return self.supports_pubsub

        # Check server info for other features
        return feature in self.server_info.get('modules', {}) or \
               feature in self.server_info.get('features', {})
