# Worker Queue declarations

cdef class WorkerQueue:
    cdef str queue_name
    cdef object redis_client
    cdef int max_workers
    cdef str queue_type
    cdef list workers
    cdef object executor
    cdef object lock
    cdef bint is_running
    cdef str queue_key
    cdef str processing_key
    cdef str completed_key
    cdef str failed_key

    cdef object _worker_loop(self, int worker_id)
    cdef dict _get_scheduled_task(self)
    cdef void _process_task(self, dict task, int worker_id)
