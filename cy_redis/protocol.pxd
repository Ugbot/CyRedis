# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Protocol Support
"""

from cy_redis.cy_redis_client cimport redisReply

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

# RESP parser
cdef class RESPParser:
    cdef int protocol_version
    cdef PushMessageHandler push_handler

# Protocol negotiator
cdef class ProtocolNegotiator:
    cdef object redis_client
    cdef int preferred_version

# Connection state
cdef class ConnectionState:
    cdef int protocol_version
    cdef bint supports_resp3
    cdef bint supports_pipelining
    cdef bint supports_scripts
    cdef bint supports_pubsub
    cdef dict server_info
