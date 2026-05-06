# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c

"""
CyPhysics — Cython wrapper for the CYPHYS.* Redis module commands.

Commands:
    CYPHYS.AABB    ax ay aw ah bx by bw bh        (center + half-extents)
    CYPHYS.SWEEP   ent_key obs_key vx vy dt cell_size
    CYPHYS.CIRCLE  spatial_key cx cy radius [limit]
    CYPHYS.RESOLVE ent_a_key ent_b_key [min_sep]
"""


cdef class CyPhysics:
    """
    Physics helpers backed by the CYPHYS Redis C module.

    All methods are stateless with respect to this object — they call Redis
    commands and return results.
    """

    def __cinit__(self, object redis_client):
        self.redis = redis_client

    cpdef bint aabb(self, double ax, double ay, double aw, double ah,
                   double bx, double by, double bw, double bh):
        """Return True if two axis-aligned bounding boxes overlap.

        Arguments are center-x, center-y, half-width, half-height for each box.
        """
        result = self.redis.execute_command([
            "CYPHYS.AABB",
            str(ax), str(ay), str(aw), str(ah),
            str(bx), str(by), str(bw), str(bh),
        ])
        return bool(result)

    cpdef dict sweep(self, str ent_key, str obs_key,
                    double vx, double vy, double dt_ms, double cell_size=1.0):
        """Swept-AABB movement check.

        Moves the entity at ``ent_key`` by (vx, vy)*dt along the obstacle grid
        HASH at ``obs_key``.  Returns a dict with keys:
            new_x, new_y  — final position (string)
            hit           — 1 if a wall was struck, 0 otherwise
            nx, ny        — collision normal (strings "0.0", "1.0", "-1.0")
        """
        raw = self.redis.execute_command([
            "CYPHYS.SWEEP", ent_key, obs_key,
            str(vx), str(vy), str(dt_ms), str(cell_size),
        ])
        if not raw or len(raw) < 5:
            return {"new_x": "0", "new_y": "0", "hit": 0, "nx": "0", "ny": "0"}
        return {
            "new_x": raw[0],
            "new_y": raw[1],
            "hit":   int(raw[2]) if raw[2] is not None else 0,
            "nx":    raw[3],
            "ny":    raw[4],
        }

    cpdef list circle_query(self, str spatial_key,
                            double cx, double cy, double radius, int limit=50):
        """Query entities within ``radius`` of (cx, cy) using the spatial ZSET.

        Returns a list of (entity_id, distance_str) tuples sorted by distance.
        """
        raw = self.redis.execute_command([
            "CYPHYS.CIRCLE", spatial_key,
            str(cx), str(cy), str(radius), str(limit),
        ])
        if not raw:
            return []
        results = []
        cdef int i
        for i in range(0, len(raw) - 1, 2):
            results.append((raw[i], raw[i + 1]))
        return results

    cpdef tuple resolve(self, str ent_a_key, str ent_b_key, double min_sep=0.1):
        """Resolve overlap between two entities stored as Redis HASHes.

        Reads x/y/w/h from each hash, computes the minimum-separation push,
        and writes the corrected positions back.

        Returns (dx, dy) — the displacement applied to entity A (entity B
        gets the equal-and-opposite push).
        """
        raw = self.redis.execute_command([
            "CYPHYS.RESOLVE", ent_a_key, ent_b_key, str(min_sep),
        ])
        if not raw or len(raw) < 2:
            return (0.0, 0.0)
        try:
            return (float(raw[0]), float(raw[1]))
        except (ValueError, TypeError):
            return (0.0, 0.0)
