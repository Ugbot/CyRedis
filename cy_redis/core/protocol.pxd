# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Protocol Support
"""

# Forward declaration - declare redisReply here to avoid circular import
cdef extern from "hiredis.h":
    ctypedef struct redisReply:
        int type
        long long integer
        double dval
        size_t len
        char *str
        size_t elements
        redisReply **element

# Protocol constants
DEF RESP2 = 2
DEF RESP3 = 3

# RESP3 reply types
DEF REDIS_REPLY_DOUBLE = 7
DEF REDIS_REPLY_BOOL = 8
DEF REDIS_REPLY_MAP = 9
DEF REDIS_REPLY_SET = 10
DEF REDIS_REPLY_PUSH = 11
DEF REDIS_REPLY_VERBATIM = 12
DEF REDIS_REPLY_BIGNUM = 13
DEF REDIS_REPLY_ATTRIBUTE = 14

# Push message handler
cdef class PushMessageHandler:
    cdef list push_messages
    cdef object push_callback

    cpdef void set_callback(self, callback)
    cpdef void handle_push_message(self, list message)
    cpdef list get_pending_messages(self)

# RESP parser
cdef class RESPParser:
    cdef int protocol_version
    cdef PushMessageHandler push_handler

    cpdef void set_protocol_version(self, int version)
    cpdef object parse_reply(self, redisReply *reply)
    cdef object _parse_reply_recursive(self, redisReply *reply)
    cdef object _parse_string(self, redisReply *reply)
    cdef object _parse_status(self, redisReply *reply)
    cdef object _parse_error(self, redisReply *reply)
    cdef list _parse_array(self, redisReply *reply)
    cdef dict _parse_map(self, redisReply *reply)
    cdef set _parse_set(self, redisReply *reply)
    cdef object _parse_verbatim(self, redisReply *reply)
    cdef object _parse_bignum(self, redisReply *reply)

# Protocol negotiator
cdef class ProtocolNegotiator:
    cdef object redis_client
    cdef int preferred_version

    cpdef int negotiate_protocol(self)
    cpdef void set_server_protocol(self, int version)

# Connection state
cdef class ConnectionState:
    cdef int protocol_version
    cdef bint supports_resp3
    cdef bint supports_pipelining
    cdef bint supports_scripts
    cdef bint supports_pubsub
    cdef dict server_info

    cpdef void update_from_hello(self, dict hello_response)
    cpdef void update_from_info(self, str info_response)
    cpdef bint supports_feature(self, str feature)
