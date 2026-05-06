# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c

"""
CyGameModule — loads and manages the cy_game Redis C module (cy_game.so).
Provides the bridge between Python and FLECS.* / CYPATH.* / CYPHYS.* / CYGOAP.*
"""

import os

_DEFAULT_SO_NAME = "cy_game.so"
_DEFAULT_SO_DIR  = os.path.join(os.path.dirname(__file__), "module")


cdef class CyGameModule:
    """
    Manages the cy_game Redis module lifecycle.

    Typical usage:
        mod = CyGameModule(redis_client)
        mod.load()               # MODULE LOAD cy_game.so
        mod.init_world("w1")     # FLECS.INIT w1
        n = mod.restore_world("w1")  # reload persisted entities
        stats = mod.get_stats("w1")
    """

    def __cinit__(self, object redis_client):
        self.redis = redis_client
        self._module_path = ""
        self._loaded = False

    cpdef void load(self, str module_path=""):
        """Load cy_game.so into Redis via MODULE LOAD.

        module_path defaults to cyredis_game/module/cy_game.so next to this file.
        Silently ignores 'already loaded' errors so callers can be idempotent.
        """
        if not module_path:
            module_path = os.path.join(_DEFAULT_SO_DIR, _DEFAULT_SO_NAME)
        module_path = os.path.abspath(module_path)
        if not os.path.exists(module_path):
            raise FileNotFoundError(
                f"cy_game.so not found at {module_path}. "
                "Build it first: make -C cyredis_game/module"
            )
        try:
            self.redis.execute_command(["MODULE", "LOAD", module_path])
        except Exception as e:
            msg = str(e).lower()
            if "already" not in msg and "loaded" not in msg:
                raise
        self._module_path = module_path
        self._loaded = True

    cpdef void unload(self):
        """Unload the cy_game module from Redis."""
        try:
            self.redis.execute_command(["MODULE", "UNLOAD", "cy_game"])
        except Exception:
            pass
        self._loaded = False

    cpdef void init_world(self, str world_id):
        """Create a FLECS world (idempotent — ignores 'already exists' error)."""
        try:
            self.redis.execute_command(["FLECS.INIT", world_id])
        except Exception as e:
            if "already" not in str(e).lower():
                raise

    cpdef int restore_world(self, str world_id):
        """Restore FLECS world from persisted Redis HASHes.

        Returns the number of entities loaded.
        Auto-inits the world first if it does not exist yet.
        """
        self.init_world(world_id)
        result = self.redis.execute_command(["FLECS.RESTORE", world_id])
        if isinstance(result, int):
            return result
        try:
            return int(result)
        except Exception:
            return 0

    cpdef dict get_stats(self, str world_id):
        """Return a dict of stats for a FLECS world."""
        raw = self.redis.execute_command(["FLECS.STATS", world_id])
        if not raw or not isinstance(raw, list) or len(raw) < 2:
            return {}
        result = {}
        for i in range(0, len(raw) - 1, 2):
            result[raw[i]] = raw[i + 1]
        return result

    cpdef bint is_loaded(self):
        """Return True if the module appears to be loaded."""
        return self._loaded
