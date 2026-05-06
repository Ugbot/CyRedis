# cython: language_level=3

cdef class CyFunctionChain:
    cdef object redis
    cdef object func_mgr
    cdef readonly object _steps

    cpdef CyFunctionChain add_fcall(self, str fn_name, list keys, list args)
    cpdef CyFunctionChain add_command(self, list cmd)
    cpdef list execute(self)
    cpdef void reset(self)
