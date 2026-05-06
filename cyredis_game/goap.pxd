# cython: language_level=3

cdef class CyGOAP:
    cdef object redis
    cdef readonly str ws_key
    cdef readonly str actions_key

    cpdef void set_state(self, dict facts)
    cpdef void define_action(self, str name, dict preconditions,
                             dict effects, int cost=*)
    cpdef list plan(self, str goal_fact, str goal_value, int max_depth=*)
    cpdef dict apply_action(self, str action_name)
    cpdef dict get_state(self)
