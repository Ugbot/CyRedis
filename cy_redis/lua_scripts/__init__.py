"""Lua scripts bundled with cy-redis.

The ``.lua`` files sit next to this module and are installed as package data,
so they are reachable from an installed wheel as well as from a checkout::

    from cy_redis.lua_scripts import script_source
    sha = client.script_load(script_source("rate_limiter"))

See ``README.md`` in this directory for each script's KEYS/ARGV contract.
"""

import os
from typing import List

__all__ = ["available_scripts", "script_path", "script_source"]

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def available_scripts() -> List[str]:
    """Names of the bundled scripts, without the ``.lua`` suffix."""
    return sorted(
        os.path.splitext(entry)[0]
        for entry in os.listdir(_SCRIPT_DIR)
        if entry.endswith(".lua")
    )


def script_path(name: str) -> str:
    """Absolute path of the bundled script ``name`` (with or without ``.lua``)."""
    filename = name if name.endswith(".lua") else name + ".lua"
    path = os.path.join(_SCRIPT_DIR, filename)
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"no bundled Lua script named {name!r}; "
            f"available: {', '.join(available_scripts())}"
        )
    return path


def script_source(name: str) -> str:
    """Source of the bundled script ``name``."""
    with open(script_path(name), "r", encoding="utf-8") as handle:
        return handle.read()
