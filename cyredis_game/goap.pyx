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

    def __cinit__(self, object redis_client, str ws_key, str actions_key):
        self.redis       = redis_client
        self.ws_key      = ws_key
        self.actions_key = actions_key

    cpdef void set_state(self, dict facts):
        """Set one or more world-state facts (string values "0" or "1").

        Accepts True/False/1/0 and coerces to "1"/"0".
        """
        if not facts:
            return
        args = [self.ws_key]
        for k, v in facts.items():
            args.append(str(k))
            args.append("1" if v else "0")
        self.redis.execute_command(["CYGOAP.SETSTATE"] + args)

    cpdef void define_action(self, str name, dict preconditions,
                             dict effects, int cost=1):
        """Register an action in the shared actions HASH.

        ``preconditions`` and ``effects`` are dicts of {fact: bool}.
        """
        pre_json = json.dumps({k: bool(v) for k, v in preconditions.items()})
        eff_json = json.dumps({k: bool(v) for k, v in effects.items()})
        self.redis.execute_command([
            "CYGOAP.DEFACTION", self.actions_key, name,
            pre_json, eff_json, str(cost),
        ])

    cpdef list plan(self, str goal_fact, str goal_value, int max_depth=10):
        """Run the GOAP planner and return the action sequence.

        Returns a list of action name strings, or [] if no plan found.
        ``goal_value`` should be "1" or "0".
        """
        result = self.redis.execute_command([
            "CYGOAP.PLAN", self.ws_key, self.actions_key,
            goal_fact, goal_value, str(max_depth),
        ])
        if not result:
            return []
        return [str(a) for a in result]

    cpdef dict apply_action(self, str action_name):
        """Apply an action: check preconditions, apply effects, update ws_key.

        Returns {"success": True} or {"success": False, "reason": "..."}.
        """
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
        raw = self.redis.execute_command(["HGETALL", self.ws_key])
        if not raw:
            return {}
        state = {}
        cdef int i
        for i in range(0, len(raw) - 1, 2):
            state[raw[i]] = raw[i + 1] == "1"
        return state
