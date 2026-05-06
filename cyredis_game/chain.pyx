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

    def __cinit__(self, object redis_client, object func_mgr):
        self.redis    = redis_client
        self.func_mgr = func_mgr
        self._steps   = []

    cpdef CyFunctionChain add_fcall(self, str fn_name, list keys, list args):
        """Append an FCALL step.

        Parameters
        ----------
        fn_name : str
            Registered Lua function name (e.g. ``"tick_step"``).
        keys : list[str]
        args : list[str]
        """
        # Build FCALL command: FCALL fn_name #keys key... arg...
        cmd = ["FCALL", fn_name, str(len(keys))] + [str(k) for k in keys] + [str(a) for a in args]
        self._steps.append(cmd)
        return self

    cpdef CyFunctionChain add_command(self, list cmd):
        """Append a raw Redis command (list of strings)."""
        self._steps.append([str(x) for x in cmd])
        return self

    cpdef list execute(self):
        """Execute all steps as a pipeline and return a list of results."""
        if not self._steps:
            return []
        results = []
        # Use pipelining when the Redis client supports it; fall back to
        # individual execute_command calls otherwise.
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
        return results

    cpdef void reset(self):
        """Clear pending steps without executing them."""
        self._steps = []
