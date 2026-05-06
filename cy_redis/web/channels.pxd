# cython: language_level=3

from cy_redis.core.cy_redis_client cimport CyRedisClient


cdef class CyChannelConnection:
    # Public read-only fields
    cdef readonly str conn_id

    # Private Python-object fields
    cdef object websocket             # ASGI WebSocket
    cdef public set _channels         # set[str] — public so CyChannelManager can mutate it
    cdef object _send_lock            # asyncio.Lock
    cdef public bint _closed          # public so tests and disconnect() can write it

    cpdef set get_channels(self)


cdef class CyChannelManager:
    cdef object _redis                   # CyRedisClient or compatible duck-type
    cdef public dict _connections        # conn_id -> CyChannelConnection (local)
    cdef dict _channel_members           # channel -> set[conn_id]         (local index)
    cdef object _psub_task               # asyncio.Task
    cdef object _lock                    # asyncio.Lock
    cdef str _key_prefix                 # default "cy:chan"
    cdef int _stream_maxlen              # XADD MAXLEN trim value
    cdef public bint _running            # public for tests / external start/stop checks
    cdef public bint _routing_loaded     # public so tests can skip Lua load
