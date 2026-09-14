# cython: language_level=3

cdef class CyGameModule:
    cdef object redis
    cdef str _module_path
    cdef bint _loaded

    cpdef void load(self, str module_path=*)
    cpdef void unload(self)
    cpdef void init_world(self, str world_id)
    cpdef int restore_world(self, str world_id)
    cpdef dict get_stats(self, str world_id)
    cpdef bint is_loaded(self)
