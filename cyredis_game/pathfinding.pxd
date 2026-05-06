# cython: language_level=3

cdef class CyPathfinder:
    cdef object redis
    cdef readonly str grid_key

    cpdef void set_cell(self, int x, int y, bint blocked)
    cpdef void clear_grid(self)
    cpdef list find_path(self, int sx, int sy, int gx, int gy, int max_steps=*)
