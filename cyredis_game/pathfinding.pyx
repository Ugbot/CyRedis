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
        self.redis    = redis_client
        self.grid_key = grid_key

    cpdef void set_cell(self, int x, int y, bint blocked):
        """Mark or unmark a grid cell as blocked."""
        self.redis.execute_command([
            "CYPATH.SET", self.grid_key,
            str(x), str(y), "1" if blocked else "0"
        ])

    cpdef void clear_grid(self):
        """Remove all obstacles from the grid (DEL the hash key)."""
        self.redis.execute_command(["CYPATH.CLEAR", self.grid_key])

    cpdef list find_path(self, int sx, int sy, int gx, int gy, int max_steps=1024):
        """Run A* and return the path as a list of (x, y) tuples.

        Returns an empty list if no path exists or start==goal.
        """
        result = self.redis.execute_command([
            "CYPATH.FIND", self.grid_key,
            str(sx), str(sy), str(gx), str(gy), str(max_steps)
        ])
        if not result:
            return []
        # result is a flat list [x0_str, y0_str, x1_str, y1_str, ...]
        path = []
        cdef int i
        for i in range(0, len(result) - 1, 2):
            try:
                path.append((int(result[i]), int(result[i + 1])))
            except (ValueError, TypeError):
                pass
        return path
