# Session Manager declarations

cdef class SessionManager:
    cdef object redis_client
    cdef int session_timeout
    cdef int cleanup_interval
    cdef str sessions_key
    cdef str users_sessions_key
    cdef long last_cleanup
