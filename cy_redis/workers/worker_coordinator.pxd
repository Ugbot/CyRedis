# Worker Coordinator declarations

cdef class WorkerCoordinator:
    cdef object redis_client
    cdef str coordinator_id
    cdef str worker_status_key
    cdef str dead_workers_key
    cdef str workload_distribution_key

    cdef void _redistribute_dead_worker_workload(self, str worker_id)
