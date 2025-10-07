# Multi-Session Tracker declarations

cdef class MultiSessionTracker:
    cdef object redis_client
    cdef SessionManager session_manager
    cdef str sessions_key
