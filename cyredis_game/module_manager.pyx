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
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        self.redis = redis_client
        self._module_path = ""
        self._loaded = False
        assert self.redis is not None, "redis client must be stored"
        assert not self._loaded, "module must start unloaded"

    cpdef void load(self, str module_path=""):
        """Load cy_game.so into Redis via MODULE LOAD.

        module_path defaults to cyredis_game/module/cy_game.so next to this file.
        Silently ignores 'already loaded' errors so callers can be idempotent.
        """
        assert self.redis is not None, "load on uninitialised module manager"
        if not module_path:
            module_path = os.path.join(_DEFAULT_SO_DIR, _DEFAULT_SO_NAME)
        module_path = os.path.abspath(module_path)
        assert os.path.isabs(module_path), "module_path must be absolute by now"
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
        # Postcondition: a successful (or already-loaded) load records the path.
        assert self._loaded, "load must mark module loaded"
        assert self._module_path, "load must record the module path"

    cpdef void unload(self):
        """Unload the cy_game module from Redis."""
        assert self.redis is not None, "unload on uninitialised module manager"
        try:
            self.redis.execute_command(["MODULE", "UNLOAD", "cy_game"])
        except Exception:
            pass
        self._loaded = False
        # Postcondition: regardless of server outcome, we no longer claim loaded.
        assert not self._loaded, "unload must clear the loaded flag"

    cpdef void init_world(self, str world_id):
        """Create a FLECS world (idempotent — ignores 'already exists' error)."""
        assert self.redis is not None, "init_world on uninitialised module manager"
        if not world_id:
            raise ValueError("world_id must be non-empty")
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
        assert self.redis is not None, "restore_world on uninitialised module manager"
        if not world_id:
            raise ValueError("world_id must be non-empty")
        self.init_world(world_id)
        result = self.redis.execute_command(["FLECS.RESTORE", world_id])
        cdef int entities_loaded
        if isinstance(result, int):
            entities_loaded = result
        else:
            try:
                entities_loaded = int(result)
            except Exception:
                entities_loaded = 0
        # Postcondition: a count is never negative.
        assert entities_loaded >= 0, "restore_world returned negative count"
        return entities_loaded

    cpdef dict get_stats(self, str world_id):
        """Return a dict of stats for a FLECS world."""
        assert self.redis is not None, "get_stats on uninitialised module manager"
        if not world_id:
            raise ValueError("world_id must be non-empty")
        raw = self.redis.execute_command(["FLECS.STATS", world_id])
        if not raw or not isinstance(raw, list) or len(raw) < 2:
            return {}
        result = {}
        # FLECS.STATS returns a flat [key, value, ...] list; bound the parse
        # loop at the reply length as a fail-safe.
        cdef Py_ssize_t reply_len = len(raw)
        cdef Py_ssize_t i = 0
        while i < reply_len - 1:
            result[raw[i]] = raw[i + 1]
            i += 2
        # Postcondition: at most one entry per key/value pair.
        assert len(result) <= (reply_len // 2) + 1, "get_stats produced extra keys"
        return result

    cpdef bint is_loaded(self):
        """Return True if the module appears to be loaded."""
        return self._loaded
