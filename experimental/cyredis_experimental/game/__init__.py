"""
CyRedis Game Engine - Authoritative ECS on Redis
A game server platform built on top of CyRedis
"""

from .game_engine import GameEngine, create_game_engine, run_zone_worker

try:
    from .module_manager import CyGameModule
    from .pathfinding import CyPathfinder
    from .physics import CyPhysics
    from .goap import CyGOAP
    from .chain import CyFunctionChain
    _SUBSYSTEMS_AVAILABLE = True
except ImportError:
    # C extensions not yet built — game engine core still works
    _SUBSYSTEMS_AVAILABLE = False
    CyGameModule = None
    CyPathfinder = None
    CyPhysics = None
    CyGOAP = None
    CyFunctionChain = None

__version__ = "0.1.0"
__all__ = [
    "GameEngine",
    "create_game_engine",
    "run_zone_worker",
    "CyGameModule",
    "CyPathfinder",
    "CyPhysics",
    "CyGOAP",
    "CyFunctionChain",
]
