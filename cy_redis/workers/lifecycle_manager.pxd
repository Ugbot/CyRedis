# Lifecycle Manager declarations

cdef class LifecycleManager:
    cdef object redis_client
    cdef list startup_hooks
    cdef list shutdown_hooks
    cdef bint is_initialized
    cdef str worker_id
    cdef str worker_status_key
    cdef str workload_transfer_key
    cdef str heartbeat_key

    cdef str _generate_worker_id(self)
    cdef void _register_worker(self)
    cdef void _update_heartbeat(self)
    cdef void _mark_worker_dead(self)
    cdef void _graceful_shutdown(self)
    cdef void _immediate_shutdown(self)
    cdef void _yield_workload(self)
    cdef dict _get_active_workload(self)
    cdef list _get_available_workers(self)
    cdef void _distribute_workload(self, dict workload, list available_workers)
    cdef void _wait_for_tasks(self, int timeout)
    cdef void _transfer_session_state(self)
    cdef void _update_worker_status(self, str status)
