# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c

"""
CyPathfinder — Cython wrapper for the CYPATH.* Redis module commands.

Grid is stored as a sparse Redis HASH where the presence of field "x,y"
with value "1" means the cell is blocked.  Absent or "0" = passable.

Commands used:
    CYPATH.SET   grid_key x y 1|0
    CYPATH.CLEAR grid_key
    CYPATH.FIND  grid_key sx sy gx gy [max_steps]
"""


cdef class CyPathfinder:
    """
    A* pathfinder backed by the CYPATH Redis C module.

    Parameters
    ----------
    redis_client : CyRedisClient
        Active Redis connection.
    grid_key : str
        Redis key for the grid obstacle HASH, e.g.
        ``"cy:nav:{world:zone}:grid"``.
    """

    def __cinit__(self, object redis_client, str grid_key):
        # Precondition: a usable client and a non-empty grid key (caller error).
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if not grid_key:
            raise ValueError("grid_key must be a non-empty string")
        self.redis    = redis_client
        self.grid_key = grid_key
        assert self.redis is not None, "redis client must be stored"
        assert self.grid_key, "grid_key must be stored non-empty"

    cpdef void set_cell(self, int x, int y, bint blocked):
        """Mark or unmark a grid cell as blocked."""
        # Invariant: the object was constructed with a live client + key.
        assert self.redis is not None, "set_cell on uninitialised pathfinder"
        assert self.grid_key, "set_cell with empty grid_key"
        self.redis.execute_command([
            "CYPATH.SET", self.grid_key,
            str(x), str(y), "1" if blocked else "0"
        ])

    cpdef void clear_grid(self):
        """Remove all obstacles from the grid (DEL the hash key)."""
        assert self.redis is not None, "clear_grid on uninitialised pathfinder"
        assert self.grid_key, "clear_grid with empty grid_key"
        self.redis.execute_command(["CYPATH.CLEAR", self.grid_key])

    cpdef list find_path(self, int sx, int sy, int gx, int gy, int max_steps=1024):
        """Run A* and return the path as a list of (x, y) tuples.

        Returns an empty list if no path exists or start==goal.

        ``max_steps`` is forwarded to the C module as the A* search cap; it
        also bounds the size of the reply we are willing to parse here.
        """
        # Preconditions (caller error → raise; impossible state → assert).
        assert self.redis is not None, "find_path on uninitialised pathfinder"
        assert self.grid_key, "find_path with empty grid_key"
        if max_steps <= 0:
            raise ValueError("max_steps must be positive")

        result = self.redis.execute_command([
            "CYPATH.FIND", self.grid_key,
            str(sx), str(sy), str(gx), str(gy), str(max_steps)
        ])
        if not result:
            return []
        # result is a flat list [x0_str, y0_str, x1_str, y1_str, ...].
        # A correct path has at most max_steps+1 nodes; bound the parse loop
        # at a generous multiple as a fail-safe against a malformed reply
        # rather than trusting len(result) blindly.
        cdef Py_ssize_t reply_len = len(result)
        cdef Py_ssize_t max_nodes = (<Py_ssize_t>max_steps + 1)
        cdef Py_ssize_t node_cap = max_nodes * 4 + 16
        path = []
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t pairs_parsed = 0
        while i < reply_len - 1 and pairs_parsed < node_cap:
            try:
                path.append((int(result[i]), int(result[i + 1])))
            except (ValueError, TypeError):
                pass
            i += 2
            pairs_parsed += 1
        # Postcondition: we never emit more nodes than the parse cap allowed.
        assert len(path) <= node_cap, "find_path produced more nodes than cap"
        return path
