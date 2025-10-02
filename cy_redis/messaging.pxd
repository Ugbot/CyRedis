# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Messaging Components
"""

# Reliable Queue
cdef class CyReliableQueue:
    cdef object redis  # Redis client
    cdef str queue_name
    cdef int visibility_timeout
    cdef int max_retries
    cdef str dead_letter_queue
    cdef str pending_key
    cdef str processing_key
    cdef str failed_key
    cdef str dead_key
    cdef object executor

# Additional classes will be declared here as they are implemented