# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language=c

"""
High-Performance RESP2/3 Protocol Implementation
Supports Redis protocol versions 2 and 3 with auto-negotiation
"""

import time
from typing import List, Dict, Any, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor

# Import hiredis types
cdef extern from "hiredis/hiredis.h":
    ctypedef struct redisReply:
        int type
        long long integer
        double dval
        size_t len
        char *str
        size_t elements
        redisReply **element

# RESP3 reply types (extending RESP2)
DEF REDIS_REPLY_STRING = 1
DEF REDIS_REPLY_ARRAY = 2
DEF REDIS_REPLY_INTEGER = 3
DEF REDIS_REPLY_NIL = 4
DEF REDIS_REPLY_STATUS = 5
DEF REDIS_REPLY_ERROR = 6

# RESP3 additional types
DEF REDIS_REPLY_DOUBLE = 7
DEF REDIS_REPLY_BOOL = 8
DEF REDIS_REPLY_MAP = 9
DEF REDIS_REPLY_SET = 10
DEF REDIS_REPLY_PUSH = 11
DEF REDIS_REPLY_VERBATIM = 12
DEF REDIS_REPLY_BIGNUM = 13
DEF REDIS_REPLY_ATTRIBUTE = 14

# Protocol version constants
DEF RESP2 = 2
DEF RESP3 = 3

# Exception classes
class RedisProtocolError(Exception):
    """Protocol parsing error"""
    pass

class RedisConnectionError(Exception):
    """Connection-related error"""
    pass

# RESP3 Push message handler
cdef class PushMessageHandler:
    """
    Handles RESP3 push messages (server-initiated messages)
    """

    cdef list push_messages
    cdef object push_callback

    def __cinit__(self):
        self.push_messages = []
        self.push_callback = None

    cpdef void set_callback(self, callback):
        """Set callback for push messages"""
        self.push_callback = callback

    cpdef void handle_push_message(self, list message):
        """Handle incoming push message"""
        self.push_messages.append({
            'timestamp': time.time(),
            'message': message
        })

        if self.push_callback:
            try:
                self.push_callback(message)
            except Exception as e:
                # Log error but don't crash
                print(f"Push message callback error: {e}")

    cpdef list get_pending_messages(self):
        """Get pending push messages"""
        cdef list messages = self.push_messages.copy()
        self.push_messages.clear()
        return messages

# High-performance RESP protocol parser
cdef class RESPParser:
    """
    High-performance RESP2/3 parser with support for all data types
    """

    cdef int protocol_version
    cdef PushMessageHandler push_handler

    def __cinit__(self, int protocol_version=RESP2):
        self.protocol_version = protocol_version
        self.push_handler = PushMessageHandler()

    cpdef void set_protocol_version(self, int version):
        """Set protocol version (2 or 3)"""
        if version not in (RESP2, RESP3):
            raise ValueError("Protocol version must be 2 or 3")
        self.protocol_version = version

    cpdef object parse_reply(self, redisReply *reply):
        """
        Parse Redis reply with full RESP2/3 support
        """
        if reply.type == REDIS_REPLY_PUSH and self.protocol_version >= RESP3:
            # Handle push message
            cdef list push_data = self._parse_reply_recursive(reply)
            self.push_handler.handle_push_message(push_data)
            return None  # Push messages don't return values to caller

        return self._parse_reply_recursive(reply)

    cdef object _parse_reply_recursive(self, redisReply *reply):
        """Recursive reply parsing"""
        if reply.type == REDIS_REPLY_STRING:
            return self._parse_string(reply)
        elif reply.type == REDIS_REPLY_ARRAY:
            return self._parse_array(reply)
        elif reply.type == REDIS_REPLY_INTEGER:
            return reply.integer
        elif reply.type == REDIS_REPLY_DOUBLE and self.protocol_version >= RESP3:
            return reply.dval
        elif reply.type == REDIS_REPLY_BOOL and self.protocol_version >= RESP3:
            return bool(reply.integer)
        elif reply.type == REDIS_REPLY_NIL:
            return None
        elif reply.type == REDIS_REPLY_STATUS:
            return self._parse_status(reply)
        elif reply.type == REDIS_REPLY_ERROR:
            return self._parse_error(reply)
        elif reply.type == REDIS_REPLY_MAP and self.protocol_version >= RESP3:
            return self._parse_map(reply)
        elif reply.type == REDIS_REPLY_SET and self.protocol_version >= RESP3:
            return self._parse_set(reply)
        elif reply.type == REDIS_REPLY_VERBATIM and self.protocol_version >= RESP3:
            return self._parse_verbatim(reply)
        elif reply.type == REDIS_REPLY_BIGNUM and self.protocol_version >= RESP3:
            return self._parse_bignum(reply)
        elif reply.type == REDIS_REPLY_ATTRIBUTE and self.protocol_version >= RESP3:
            # Attributes are metadata, parse the actual value
            return self._parse_reply_recursive(reply.element[1]) if reply.elements >= 2 else None
        else:
            # Unknown type, return as string if possible
            if reply.str:
                return reply.str.decode('utf-8', errors='replace')
            return None

    cdef object _parse_string(self, redisReply *reply):
        """Parse string reply"""
        if reply.str and reply.len > 0:
            return reply.str[:reply.len].decode('utf-8', errors='replace')
        return ""

    cdef object _parse_status(self, redisReply *reply):
        """Parse status reply"""
        if reply.str and reply.len > 0:
            return reply.str[:reply.len].decode('utf-8', errors='replace')
        return "OK"

    cdef object _parse_error(self, redisReply *reply):
        """Parse error reply"""
        cdef str error_msg = ""
        if reply.str and reply.len > 0:
            error_msg = reply.str[:reply.len].decode('utf-8', errors='replace')
        raise RedisProtocolError(error_msg)

    cdef list _parse_array(self, redisReply *reply):
        """Parse array reply"""
        cdef list result = []
        cdef int i
        for i in range(reply.elements):
            result.append(self._parse_reply_recursive(reply.element[i]))
        return result

    cdef dict _parse_map(self, redisReply *reply):
        """Parse RESP3 map (key-value pairs)"""
        cdef dict result = {}
        cdef int i
        # Maps come as flat key-value pairs
        for i in range(0, reply.elements, 2):
            if i + 1 < reply.elements:
                cdef object key = self._parse_reply_recursive(reply.element[i])
                cdef object value = self._parse_reply_recursive(reply.element[i + 1])
                result[key] = value
        return result

    cdef set _parse_set(self, redisReply *reply):
        """Parse RESP3 set"""
        cdef set result = set()
        cdef int i
        for i in range(reply.elements):
            result.add(self._parse_reply_recursive(reply.element[i]))
        return result

    cdef object _parse_verbatim(self, redisReply *reply):
        """Parse RESP3 verbatim string (format:encoding:data)"""
        if reply.elements >= 1:
            cdef object content = self._parse_reply_recursive(reply.element[0])
            if isinstance(content, str) and ':' in content:
                # Format: "format:encoding:data"
                parts = content.split(':', 2)
                if len(parts) >= 3:
                    return {
                        'format': parts[0],
                        'encoding': parts[1],
                        'data': parts[2]
                    }
            return content
        return ""

    cdef object _parse_bignum(self, redisReply *reply):
        """Parse RESP3 big number"""
        if reply.str and reply.len > 0:
            # Return as string to preserve precision
            return reply.str[:reply.len].decode('utf-8', errors='replace')
        return "0"

# Protocol negotiation helper
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

# Connection state tracker
cdef class ConnectionState:
    """
    Tracks connection state including protocol version and capabilities
    """

    cdef int protocol_version
    cdef bint supports_resp3
    cdef bint supports_pipelining
    cdef bint supports_scripts
    cdef bint supports_pubsub
    cdef dict server_info

    def __cinit__(self):
        self.protocol_version = RESP2
        self.supports_resp3 = False
        self.supports_pipelining = True  # Most Redis versions support this
        self.supports_scripts = True     # Most Redis versions support this
        self.supports_pubsub = True      # Most Redis versions support this
        self.server_info = {}

    cpdef void update_from_hello(self, dict hello_response):
        """Update state from HELLO command response"""
        if 'proto' in hello_response:
            self.protocol_version = hello_response['proto']
            self.supports_resp3 = self.protocol_version >= 3

        # Store server capabilities
        self.server_info = hello_response

    cpdef void update_from_info(self, str info_response):
        """Update state from INFO command response"""
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
                current_section = line[1:] if line.startswith('#') else ""
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

# Export Python interface
class RedisProtocol:
    """
    Python interface to RESP protocol handling
    """

    def __init__(self, protocol_version=RESP2):
        self._parser = RESPParser(protocol_version)
        self._negotiator = None
        self._connection_state = ConnectionState()

    def set_protocol_version(self, version):
        """Set protocol version"""
        self._parser.set_protocol_version(version)
        self._connection_state.protocol_version = version

    def negotiate_protocol(self, redis_client):
        """Negotiate protocol with server"""
        if not self._negotiator:
            self._negotiator = ProtocolNegotiator(redis_client)
        version = self._negotiator.negotiate_protocol()
        self.set_protocol_version(version)
        return version

    def set_push_callback(self, callback):
        """Set callback for push messages"""
        self._parser.push_handler.set_callback(callback)

    def get_pending_push_messages(self):
        """Get pending push messages"""
        return self._parser.push_handler.get_pending_messages()

    def parse_reply(self, reply):
        """Parse a Redis reply"""
        return self._parser.parse_reply(reply)

    @property
    def protocol_version(self):
        return self._connection_state.protocol_version

    @property
    def supports_resp3(self):
        return self._connection_state.supports_resp3
