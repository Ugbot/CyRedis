/* cy_game.h — shared types and world management API for the cy_game Redis module */
#pragma once
#include <stdint.h>
#include <string.h>

/* ── FLECS configuration (must precede flecs.h) ─────────────────────────── */
/* Disable only the addons that pull in network/REST/script dependencies.
 * Everything else (ECS core, queries, systems, pipelines) stays enabled. */
#define FLECS_NO_HTTP
#define FLECS_NO_REST
#define FLECS_NO_APP
#include "vendor/flecs.h"
#include "vendor/redismodule.h"

/* ── Game component types ────────────────────────────────────────────────── */
typedef struct { double x,  y;      } Position;
typedef struct { double vx, vy;     } Velocity;
typedef struct { int    hp, max_hp; } Health;
typedef struct { char   zone[64];   } ZoneId;
typedef struct { char   eid[128];   } EntityKey;

/* ── World management ────────────────────────────────────────────────────── */
#define CY_MAX_WORLDS      64
#define CY_ENTITY_BUCKETS  8192   /* power-of-2, open-addressing hash map */

typedef struct {
    char         eid[128];
    ecs_entity_t fid;
    int          used;
} EntitySlot;

typedef struct {
    char            id[128];
    ecs_world_t    *ecs;
    int             in_use;

    /* per-world component entity IDs */
    ecs_entity_t    comp_Position;
    ecs_entity_t    comp_Velocity;
    ecs_entity_t    comp_Health;
    ecs_entity_t    comp_ZoneId;
    ecs_entity_t    comp_EntityKey;

    /* cached movement query */
    ecs_query_t    *move_query;

    /* entity string-id → flecs entity lookup */
    EntitySlot      slots[CY_ENTITY_BUCKETS];
    int             entity_count;
} CyWorld;

/* World registry API */
CyWorld     *cy_world_get(const char *world_id);
CyWorld     *cy_world_create(RedisModuleCtx *ctx, const char *world_id);
void         cy_world_destroy(const char *world_id);

/* Entity hash-map API */
ecs_entity_t cy_entity_lookup(CyWorld *w, const char *eid);
void         cy_entity_insert(CyWorld *w, const char *eid, ecs_entity_t fid);
void         cy_entity_erase(CyWorld *w, const char *eid);

/* Helpers */
static inline unsigned int cy_hash_str(const char *s) {
    unsigned int h = 2166136261u;
    while (*s) { h ^= (unsigned char)*s++; h *= 16777619u; }
    return h;
}
