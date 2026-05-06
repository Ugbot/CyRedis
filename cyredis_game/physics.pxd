# cython: language_level=3

cdef class CyPhysics:
    cdef object redis

    cpdef bint aabb(self, double ax, double ay, double aw, double ah,
                   double bx, double by, double bw, double bh)
    cpdef dict sweep(self, str ent_key, str obs_key,
                    double vx, double vy, double dt_ms, double cell_size=*)
    cpdef list circle_query(self, str spatial_key,
                            double cx, double cy, double radius, int limit=*)
    cpdef tuple resolve(self, str ent_a_key, str ent_b_key, double min_sep=*)
