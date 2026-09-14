# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c

"""
CyGOAP — Cython wrapper for the CYGOAP.* Redis module commands.

Commands:
    CYGOAP.SETSTATE   ws_key fact val [fact val ...]
    CYGOAP.DEFACTION  actions_key name pre_json eff_json cost
    CYGOAP.PLAN       ws_key actions_key goal_fact goal_val [max_depth]
    CYGOAP.APPLY      ws_key actions_key action_name
"""

import json


cdef class CyGOAP:
    """
    Goal-Oriented Action Planning (GOAP) backed by the CYGOAP Redis C module.

    Parameters
    ----------
    redis_client : CyRedisClient
    ws_key : str
        Redis HASH key storing the agent's world-state facts.
        Convention: ``"cy:ws:{world:zone}:{agent_id}"``.
    actions_key : str
        Redis HASH key storing action definitions.
        Convention: ``"cy:actions:{world:zone}"``.
    """

    # Fail-safe cap on facts accepted in a single SETSTATE call.  GOAP world
    # states are small (tens of boolean facts); a request far above this is a
    # programmer/caller error, not a legitimate state.
    _MAX_FACTS = 4096

    def __cinit__(self, object redis_client, str ws_key, str actions_key):
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if not ws_key:
            raise ValueError("ws_key must be a non-empty string")
        if not actions_key:
            raise ValueError("actions_key must be a non-empty string")
        self.redis       = redis_client
        self.ws_key      = ws_key
        self.actions_key = actions_key
        assert self.redis is not None, "redis client must be stored"
        assert self.ws_key and self.actions_key, "keys must be stored non-empty"

    cpdef void set_state(self, dict facts):
        """Set one or more world-state facts (string values "0" or "1").

        Accepts True/False/1/0 and coerces to "1"/"0".
        """
        assert self.redis is not None, "set_state on uninitialised GOAP"
        assert self.ws_key, "set_state with empty ws_key"
        if not facts:
            return
        # Bound the fact count: a GOAP world state is small by construction.
        if len(facts) > self._MAX_FACTS:
            raise ValueError(
                f"too many facts ({len(facts)} > {self._MAX_FACTS})"
            )
        args = [self.ws_key]
        for k, v in facts.items():
            args.append(str(k))
            args.append("1" if v else "0")
        # Invariant: one key + (fact, value) pair per fact ⇒ odd-length argv.
        assert len(args) == 1 + 2 * len(facts), "set_state argv length mismatch"
        self.redis.execute_command(["CYGOAP.SETSTATE"] + args)

    cpdef void define_action(self, str name, dict preconditions,
                             dict effects, int cost=1):
        """Register an action in the shared actions HASH.

        ``preconditions`` and ``effects`` are dicts of {fact: bool}.
        """
        # Preconditions: a named action, dict-typed pre/eff, sane cost.
        assert self.redis is not None, "define_action on uninitialised GOAP"
        assert self.actions_key, "define_action with empty actions_key"
        if not name:
            raise ValueError("action name must be non-empty")
        if preconditions is None or effects is None:
            raise ValueError("preconditions and effects must be dicts, not None")
        if cost < 0:
            raise ValueError("cost must be non-negative")
        pre_json = json.dumps({k: bool(v) for k, v in preconditions.items()})
        eff_json = json.dumps({k: bool(v) for k, v in effects.items()})
        assert pre_json and eff_json, "serialised pre/eff must be non-empty JSON"
        self.redis.execute_command([
            "CYGOAP.DEFACTION", self.actions_key, name,
            pre_json, eff_json, str(cost),
        ])

    cpdef list plan(self, str goal_fact, str goal_value, int max_depth=10):
        """Run the GOAP planner and return the action sequence.

        Returns a list of action name strings, or [] if no plan found.
        ``goal_value`` should be "1" or "0".

        ``max_depth`` bounds the planner's search depth in the C module; we
        also use it to bound how long a plan we are willing to materialise as
        a fail-safe against a runaway/malformed reply.
        """
        # Preconditions: live state, a goal fact, and a positive depth bound.
        assert self.redis is not None, "plan on uninitialised GOAP"
        assert self.ws_key and self.actions_key, "plan with empty keys"
        if not goal_fact:
            raise ValueError("goal_fact must be non-empty")
        if max_depth <= 0:
            raise ValueError("max_depth must be positive")
        result = self.redis.execute_command([
            "CYGOAP.PLAN", self.ws_key, self.actions_key,
            goal_fact, goal_value, str(max_depth),
        ])
        if not result:
            return []
        # A valid plan has at most max_depth actions; cap the materialised list
        # generously so a bad reply cannot make us build an unbounded list.
        cdef Py_ssize_t reply_len = len(result)
        cdef Py_ssize_t action_cap = (<Py_ssize_t>max_depth) * 4 + 16
        plan_actions = []
        cdef Py_ssize_t i = 0
        while i < reply_len and i < action_cap:
            plan_actions.append(str(result[i]))
            i += 1
        # Postcondition: never return more actions than the parse cap.
        assert len(plan_actions) <= action_cap, "plan exceeded action cap"
        return plan_actions

    cpdef dict apply_action(self, str action_name):
        """Apply an action: check preconditions, apply effects, update ws_key.

        Returns {"success": True} or {"success": False, "reason": "..."}.
        """
        assert self.redis is not None, "apply_action on uninitialised GOAP"
        assert self.ws_key and self.actions_key, "apply_action with empty keys"
        if not action_name:
            raise ValueError("action_name must be non-empty")
        result = self.redis.execute_command([
            "CYGOAP.APPLY", self.ws_key, self.actions_key, action_name,
        ])
        if not result or len(result) < 1:
            return {"success": False, "reason": "no_response"}
        status = str(result[0])
        if status == "applied":
            return {"success": True}
        reason = str(result[1]) if len(result) > 1 else "unknown"
        return {"success": False, "reason": reason}

    cpdef dict get_state(self):
        """Read back the current world-state facts as a Python dict."""
        assert self.redis is not None, "get_state on uninitialised GOAP"
        assert self.ws_key, "get_state with empty ws_key"
        raw = self.redis.execute_command(["HGETALL", self.ws_key])
        if not raw:
            return {}
        state = {}
        # HGETALL returns a flat [field, value, ...] list; bound the parse loop
        # at the reply length as a fail-safe rather than trusting it implicitly.
        cdef Py_ssize_t reply_len = len(raw)
        cdef Py_ssize_t i = 0
        while i < reply_len - 1:
            state[raw[i]] = raw[i + 1] == "1"
            i += 2
        # Postcondition: at most one entry per field pair.
        assert len(state) <= (reply_len // 2) + 1, "get_state produced extra keys"
        return state
