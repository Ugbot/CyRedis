"""
CyRedis Game Engine - Authoritative ECS on Redis
A game server platform built on top of CyRedis
"""

from .game_engine import GameEngine, create_game_engine, run_zone_worker

__version__ = "0.1.0"
__all__ = [
    "GameEngine",
    "create_game_engine",
    "run_zone_worker",
]
