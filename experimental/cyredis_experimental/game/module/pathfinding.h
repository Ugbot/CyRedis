/* pathfinding.h — CYPATH.* Redis module commands (A* on sparse Redis HASH grid) */
#pragma once
#include "vendor/redismodule.h"

/* Register all CYPATH.* commands. Called from RedisModule_OnLoad. */
int PathfindingModule_Init(RedisModuleCtx *ctx);
