# cython: language_level=3
# distutils: language=c

"""
Header declarations for CyRedis Lua Script Manager
"""

# Optimized Lua Script Manager
cdef class CyLuaScriptManager:
    cdef object redis  # CyRedisClient
    cdef str namespace
    cdef dict loaded_scripts
    cdef dict script_versions
    cdef dict script_sources
    cdef dict script_metadata
    cdef object executor

    cdef str _compute_script_hash(self, str script)
