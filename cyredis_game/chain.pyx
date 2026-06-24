# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c

"""
CyFunctionChain — pipeline builder for batching multiple FCALL and Redis
command calls into a single pipeline execution.

Usage:
    chain = CyFunctionChain(redis_client, func_mgr)
    result = (chain
              .add_fcall("tick_step", keys=[...], args=[...])
              .add_command(["ZADD", spatial_key, score, eid])
              .execute())
"""


cdef class CyFunctionChain:
    """
    Builds and executes a pipeline of FCALL and raw Redis commands.

    All steps are accumulated in ``_steps`` and executed together via a
    Redis pipeline when ``execute()`` is called.  The pipeline is reset
    after each execute so the object can be reused.
    """

    # Fail-safe cap on pending steps in a single chain.  A pipeline far larger
    # than this is almost certainly a caller bug (e.g. an unbounded loop
    # appending steps) rather than a legitimate batch; it would also blow the
    # server's reply buffer if executed.
    _MAX_STEPS = 100000

    def __cinit__(self, object redis_client, object func_mgr):
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        self.redis    = redis_client
        self.func_mgr = func_mgr
        self._steps   = []
        assert self.redis is not None, "redis client must be stored"
        assert self._steps == [], "step list must start empty"

    cpdef CyFunctionChain add_fcall(self, str fn_name, list keys, list args):
        """Append an FCALL step.

        Parameters
        ----------
        fn_name : str
            Registered Lua function name (e.g. ``"tick_step"``).
        keys : list[str]
        args : list[str]
        """
        # Preconditions: a named function and list-typed key/arg vectors.
        if not fn_name:
            raise ValueError("fn_name must be non-empty")
        if keys is None or args is None:
            raise ValueError("keys and args must be lists, not None")
        if len(self._steps) >= self._MAX_STEPS:
            raise ValueError(f"chain exceeds max steps ({self._MAX_STEPS})")
        # Build FCALL command: FCALL fn_name #keys key... arg...
        cmd = ["FCALL", fn_name, str(len(keys))] + [str(k) for k in keys] + [str(a) for a in args]
        # Invariant: 3 fixed tokens + one per key + one per arg.
        assert len(cmd) == 3 + len(keys) + len(args), "FCALL argv length mismatch"
        self._steps.append(cmd)
        return self

    cpdef CyFunctionChain add_command(self, list cmd):
        """Append a raw Redis command (list of strings)."""
        if cmd is None:
            raise ValueError("cmd must be a list, not None")
        if len(cmd) == 0:
            raise ValueError("cmd must contain at least one token")
        if len(self._steps) >= self._MAX_STEPS:
            raise ValueError(f"chain exceeds max steps ({self._MAX_STEPS})")
        self._steps.append([str(x) for x in cmd])
        return self

    cpdef list execute(self):
        """Execute all steps as a pipeline and return a list of results."""
        assert self.redis is not None, "execute on uninitialised chain"
        if not self._steps:
            return []
        # Invariant: the step list is bounded by the add_* guards above.
        cdef Py_ssize_t step_count = len(self._steps)
        assert step_count <= self._MAX_STEPS, "step list exceeded cap"
        results = []
        # Use pipelining when the Redis client supports it; fall back to
        # individual execute_command calls otherwise.  Both loops iterate the
        # already-bounded step list, so they are themselves bounded.
        try:
            pipe = self.redis.pipeline()
            for step in self._steps:
                pipe.execute_command(step)
            results = pipe.execute()
        except AttributeError:
            # Client has no .pipeline() — execute sequentially
            for step in self._steps:
                try:
                    results.append(self.redis.execute_command(step))
                except Exception as e:
                    results.append(e)
        self._steps = []
        # Postcondition: the chain is reset and ready for reuse.
        assert self._steps == [], "chain not reset after execute"
        return results

    cpdef void reset(self):
        """Clear pending steps without executing them."""
        self._steps = []
        assert self._steps == [], "chain not cleared by reset"
