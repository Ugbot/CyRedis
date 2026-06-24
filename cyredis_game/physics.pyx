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
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        self.redis = redis_client
        assert self.redis is not None, "redis client must be stored"

    cpdef bint aabb(self, double ax, double ay, double aw, double ah,
                   double bx, double by, double bw, double bh):
        """Return True if two axis-aligned bounding boxes overlap.

        Arguments are center-x, center-y, half-width, half-height for each box.
        """
        # Half-extents are non-negative by definition (caller error otherwise).
        assert self.redis is not None, "aabb on uninitialised physics"
        if aw < 0.0 or ah < 0.0 or bw < 0.0 or bh < 0.0:
            raise ValueError("half-extents (aw, ah, bw, bh) must be non-negative")
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
        # Preconditions: keys present, cell_size strictly positive (caller error).
        assert self.redis is not None, "sweep on uninitialised physics"
        if not ent_key or not obs_key:
            raise ValueError("ent_key and obs_key must be non-empty")
        if cell_size <= 0.0:
            raise ValueError("cell_size must be positive")
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
        # Preconditions: key present, radius non-negative, limit positive.
        assert self.redis is not None, "circle_query on uninitialised physics"
        if not spatial_key:
            raise ValueError("spatial_key must be non-empty")
        if radius < 0.0:
            raise ValueError("radius must be non-negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        raw = self.redis.execute_command([
            "CYPHYS.CIRCLE", spatial_key,
            str(cx), str(cy), str(radius), str(limit),
        ])
        if not raw:
            return []
        results = []
        # The module already caps results at ``limit`` pairs; bound the parse
        # loop at that same limit as a fail-safe against a malformed reply.
        cdef Py_ssize_t reply_len = len(raw)
        cdef Py_ssize_t pair_cap = <Py_ssize_t>limit
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t pairs_parsed = 0
        while i < reply_len - 1 and pairs_parsed < pair_cap:
            results.append((raw[i], raw[i + 1]))
            i += 2
            pairs_parsed += 1
        # Postcondition: never return more than the requested limit.
        assert len(results) <= pair_cap, "circle_query exceeded requested limit"
        return results

    cpdef tuple resolve(self, str ent_a_key, str ent_b_key, double min_sep=0.1):
        """Resolve overlap between two entities stored as Redis HASHes.

        Reads x/y/w/h from each hash, computes the minimum-separation push,
        and writes the corrected positions back.

        Returns (dx, dy) — the displacement applied to entity A (entity B
        gets the equal-and-opposite push).
        """
        # Preconditions: distinct-ish keys present, min_sep non-negative.
        assert self.redis is not None, "resolve on uninitialised physics"
        if not ent_a_key or not ent_b_key:
            raise ValueError("ent_a_key and ent_b_key must be non-empty")
        if min_sep < 0.0:
            raise ValueError("min_sep must be non-negative")
        raw = self.redis.execute_command([
            "CYPHYS.RESOLVE", ent_a_key, ent_b_key, str(min_sep),
        ])
        if not raw or len(raw) < 2:
            return (0.0, 0.0)
        try:
            return (float(raw[0]), float(raw[1]))
        except (ValueError, TypeError):
            return (0.0, 0.0)
