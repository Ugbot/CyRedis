/* cy_game.c — CyRedis Game Module: FLECS-embedded Redis module
 *
 * Provides:
 *   FLECS.INIT    world_id
 *   FLECS.FINI    world_id
 *   FLECS.STATS   world_id
 *   FLECS.SPAWN   world zone eid type x y [vx vy] [hp [max_hp]]
 *   FLECS.DELETE  world eid
 *   FLECS.GETCOMP world eid component
 *   FLECS.SETCOMP world eid component field value [field value ...]
 *   FLECS.HAS     world eid component
 *   FLECS.TICK    world zone dt_ms [budget]
 *   FLECS.QUERY   world filter [zone]
 *   FLECS.RESTORE world
 *
 * Also loads: CYPATH.*, CYPHYS.*, CYGOAP.* sub-modules.
 */

/* FLECS compile-time configuration must precede cy_game.h */
#define FLECS_IMPL          /* define the implementation once, here only */

#include "cy_game.h"
#include "pathfinding.h"
#include "physics.h"
#include "goap.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ── Memory shims (ecs_size_t is int32_t) ────────────────────────────────── */
static void *cy_malloc(ecs_size_t sz)  { return RedisModule_Alloc((size_t)sz); }
static void  cy_free(void *p)          { RedisModule_Free(p); }
static void *cy_realloc(void *p, ecs_size_t sz) {
    return RedisModule_Realloc(p, (size_t)sz);
}
static void *cy_calloc(ecs_size_t n, ecs_size_t sz) {
    size_t total = (size_t)n * (size_t)sz;
    void *p = RedisModule_Alloc(total);
    if (p) memset(p, 0, total);
    return p;
}

/* ── World registry ──────────────────────────────────────────────────────── */
static CyWorld g_worlds[CY_MAX_WORLDS];

CyWorld *cy_world_get(const char *world_id) {
    for (int i = 0; i < CY_MAX_WORLDS; i++) {
        if (g_worlds[i].in_use && strcmp(g_worlds[i].id, world_id) == 0)
            return &g_worlds[i];
    }
    return NULL;
}

CyWorld *cy_world_create(RedisModuleCtx *ctx, const char *world_id) {
    CyWorld *w = NULL;
    for (int i = 0; i < CY_MAX_WORLDS; i++) {
        if (!g_worlds[i].in_use) { w = &g_worlds[i]; break; }
    }
    if (!w) {
        RedisModule_Log(ctx, "warning", "cy_game: max worlds (%d) reached", CY_MAX_WORLDS);
        return NULL;
    }
    memset(w, 0, sizeof(CyWorld));
    strncpy(w->id, world_id, sizeof(w->id) - 1);

    w->ecs = ecs_init();
    if (!w->ecs) {
        RedisModule_Log(ctx, "warning", "cy_game: ecs_init() failed for world %s", world_id);
        return NULL;
    }

    /* Register components */
    w->comp_Position  = ecs_component_init(w->ecs, &(ecs_component_desc_t){
        .entity = ecs_entity(w->ecs, { .name = "Position" }),
        .type.size = sizeof(Position), .type.alignment = ECS_ALIGNOF(Position) });
    w->comp_Velocity  = ecs_component_init(w->ecs, &(ecs_component_desc_t){
        .entity = ecs_entity(w->ecs, { .name = "Velocity" }),
        .type.size = sizeof(Velocity), .type.alignment = ECS_ALIGNOF(Velocity) });
    w->comp_Health    = ecs_component_init(w->ecs, &(ecs_component_desc_t){
        .entity = ecs_entity(w->ecs, { .name = "Health" }),
        .type.size = sizeof(Health), .type.alignment = ECS_ALIGNOF(Health) });
    w->comp_ZoneId    = ecs_component_init(w->ecs, &(ecs_component_desc_t){
        .entity = ecs_entity(w->ecs, { .name = "ZoneId" }),
        .type.size = sizeof(ZoneId), .type.alignment = ECS_ALIGNOF(ZoneId) });
    w->comp_EntityKey = ecs_component_init(w->ecs, &(ecs_component_desc_t){
        .entity = ecs_entity(w->ecs, { .name = "EntityKey" }),
        .type.size = sizeof(EntityKey), .type.alignment = ECS_ALIGNOF(EntityKey) });

    /* Cached movement query: entities that have Position + Velocity + ZoneId */
    w->move_query = ecs_query(w->ecs, {
        .terms = {
            { .id = w->comp_Position  },
            { .id = w->comp_Velocity  },
            { .id = w->comp_ZoneId    },
        }
    });

    w->in_use = 1;
    RedisModule_Log(ctx, "notice", "cy_game: world '%s' initialised", world_id);
    return w;
}

void cy_world_destroy(const char *world_id) {
    CyWorld *w = cy_world_get(world_id);
    if (!w) return;
    if (w->move_query) { ecs_query_fini(w->move_query); w->move_query = NULL; }
    if (w->ecs)        { ecs_fini(w->ecs); w->ecs = NULL; }
    memset(w, 0, sizeof(CyWorld));
}

/* ── Entity hash map ─────────────────────────────────────────────────────── */
ecs_entity_t cy_entity_lookup(CyWorld *w, const char *eid) {
    unsigned int h = cy_hash_str(eid) & (CY_ENTITY_BUCKETS - 1);
    for (int i = 0; i < CY_ENTITY_BUCKETS; i++) {
        int idx = (h + (unsigned int)i) & (CY_ENTITY_BUCKETS - 1);
        if (!w->slots[idx].used) return 0;
        if (strcmp(w->slots[idx].eid, eid) == 0) return w->slots[idx].fid;
    }
    return 0;
}

void cy_entity_insert(CyWorld *w, const char *eid, ecs_entity_t fid) {
    unsigned int h = cy_hash_str(eid) & (CY_ENTITY_BUCKETS - 1);
    for (int i = 0; i < CY_ENTITY_BUCKETS; i++) {
        int idx = (h + (unsigned int)i) & (CY_ENTITY_BUCKETS - 1);
        if (!w->slots[idx].used) {
            strncpy(w->slots[idx].eid, eid, sizeof(w->slots[idx].eid) - 1);
            w->slots[idx].fid  = fid;
            w->slots[idx].used = 1;
            w->entity_count++;
            return;
        }
    }
}

void cy_entity_erase(CyWorld *w, const char *eid) {
    unsigned int h = cy_hash_str(eid) & (CY_ENTITY_BUCKETS - 1);
    int found = -1;
    for (int i = 0; i < CY_ENTITY_BUCKETS; i++) {
        int idx = (h + (unsigned int)i) & (CY_ENTITY_BUCKETS - 1);
        if (!w->slots[idx].used) return;
        if (strcmp(w->slots[idx].eid, eid) == 0) { found = idx; break; }
    }
    if (found < 0) return;
    /* Tombstone removal: shift subsequent entries */
    w->slots[found].used = 0;
    w->entity_count--;
    int cur = found;
    for (;;) {
        int next = (cur + 1) & (CY_ENTITY_BUCKETS - 1);
        if (!w->slots[next].used) break;
        EntitySlot tmp = w->slots[next];
        w->slots[next].used = 0;
        unsigned int nh = cy_hash_str(tmp.eid) & (CY_ENTITY_BUCKETS - 1);
        /* rehash */
        for (int k = 0; k < CY_ENTITY_BUCKETS; k++) {
            int nidx = (nh + (unsigned int)k) & (CY_ENTITY_BUCKETS - 1);
            if (!w->slots[nidx].used) { w->slots[nidx] = tmp; break; }
        }
        cur = next;
    }
}

/* ── Helper: spatial ZSET score encoding ─────────────────────────────────── */
static long long spatial_score(double x, double y) {
    return (long long)(x * 1000.0) * 1000000000LL + (long long)(y * 1000.0);
}

/* ── Helper: Redis HSET a single string field ────────────────────────────── */
static void hset_double(RedisModuleCtx *ctx, RedisModuleString *key,
                        const char *field, double v) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.6f", v);
    RedisModuleCallReply *r = RedisModule_Call(ctx, "HSET", "scc", key, field, buf);
    if (r) RedisModule_FreeCallReply(r);
}

static void hset_int(RedisModuleCtx *ctx, RedisModuleString *key,
                     const char *field, int v) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", v);
    RedisModuleCallReply *r = RedisModule_Call(ctx, "HSET", "scc", key, field, buf);
    if (r) RedisModule_FreeCallReply(r);
}

static void hset_str(RedisModuleCtx *ctx, RedisModuleString *key,
                     const char *field, const char *val) {
    RedisModuleCallReply *r = RedisModule_Call(ctx, "HSET", "scc", key, field, val);
    if (r) RedisModule_FreeCallReply(r);
}

/* ── Helper: build Redis key string ─────────────────────────────────────── */
static RedisModuleString *make_key(RedisModuleCtx *ctx,
                                   const char *fmt, ...) {
    char buf[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return RedisModule_CreateString(ctx, buf, strlen(buf));
}

/* ── FLECS.INIT world_id ─────────────────────────────────────────────────── */
static int Cmd_FLECS_INIT(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    size_t len;
    const char *wid = RedisModule_StringPtrLen(argv[1], &len);
    if (cy_world_get(wid))
        return RedisModule_ReplyWithError(ctx, "ERR world already exists");
    if (!cy_world_create(ctx, wid))
        return RedisModule_ReplyWithError(ctx, "ERR world creation failed");
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── FLECS.FINI world_id ─────────────────────────────────────────────────── */
static int Cmd_FLECS_FINI(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    size_t len;
    const char *wid = RedisModule_StringPtrLen(argv[1], &len);
    if (!cy_world_get(wid))
        return RedisModule_ReplyWithError(ctx, "ERR world not found");
    cy_world_destroy(wid);
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── FLECS.STATS world_id ────────────────────────────────────────────────── */
static int Cmd_FLECS_STATS(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    size_t len;
    const char *wid = RedisModule_StringPtrLen(argv[1], &len);
    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");

    int64_t entity_count = ecs_count_id(w->ecs, 0);
    RedisModule_ReplyWithArray(ctx, 6);
    RedisModule_ReplyWithSimpleString(ctx, "entities");
    RedisModule_ReplyWithLongLong(ctx, entity_count);
    RedisModule_ReplyWithSimpleString(ctx, "indexed_entities");
    RedisModule_ReplyWithLongLong(ctx, w->entity_count);
    RedisModule_ReplyWithSimpleString(ctx, "world");
    RedisModule_ReplyWithCString(ctx, w->id);
    return REDISMODULE_OK;
}

/* ── FLECS.SPAWN world zone eid type x y [vx vy] [hp [max_hp]] ──────────── */
static int Cmd_FLECS_SPAWN(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    /* argv: 0=FLECS.SPAWN 1=world 2=zone 3=eid 4=type 5=x 6=y
     *       [7=vx 8=vy] [9=hp [10=max_hp]] */
    if (argc < 7) return RedisModule_WrongArity(ctx);

    size_t wlen, zlen, elen, tlen;
    const char *wid  = RedisModule_StringPtrLen(argv[1], &wlen);
    const char *zone = RedisModule_StringPtrLen(argv[2], &zlen);
    const char *eid  = RedisModule_StringPtrLen(argv[3], &elen);
    const char *type = RedisModule_StringPtrLen(argv[4], &tlen);

    double x, y;
    if (RedisModule_StringToDouble(argv[5], &x) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[6], &y) != REDISMODULE_OK)
        return RedisModule_ReplyWithError(ctx, "ERR invalid x/y");

    double vx = 0, vy = 0;
    if (argc >= 9) {
        if (RedisModule_StringToDouble(argv[7], &vx) != REDISMODULE_OK ||
            RedisModule_StringToDouble(argv[8], &vy) != REDISMODULE_OK)
            return RedisModule_ReplyWithError(ctx, "ERR invalid vx/vy");
    }
    int hp = 100, max_hp = 100;
    if (argc >= 10) {
        long long lhp;
        if (RedisModule_StringToLongLong(argv[9], &lhp) != REDISMODULE_OK)
            return RedisModule_ReplyWithError(ctx, "ERR invalid hp");
        hp = max_hp = (int)lhp;
    }
    if (argc >= 11) {
        long long lmhp;
        if (RedisModule_StringToLongLong(argv[10], &lmhp) != REDISMODULE_OK)
            return RedisModule_ReplyWithError(ctx, "ERR invalid max_hp");
        max_hp = (int)lmhp;
    }

    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");

    if (cy_entity_lookup(w, eid) != 0)
        return RedisModule_ReplyWithLongLong(ctx, 0);  /* already exists */

    /* Create FLECS entity */
    ecs_entity_t e = ecs_new(w->ecs);

    Position pos = { x, y };
    ecs_set_id(w->ecs, e, w->comp_Position, sizeof(Position), &pos);

    Velocity vel = { vx, vy };
    ecs_set_id(w->ecs, e, w->comp_Velocity, sizeof(Velocity), &vel);

    Health h = { hp, max_hp };
    ecs_set_id(w->ecs, e, w->comp_Health, sizeof(Health), &h);

    ZoneId zi; strncpy(zi.zone, zone, sizeof(zi.zone) - 1); zi.zone[sizeof(zi.zone)-1] = 0;
    ecs_set_id(w->ecs, e, w->comp_ZoneId, sizeof(ZoneId), &zi);

    EntityKey ek; strncpy(ek.eid, eid, sizeof(ek.eid) - 1); ek.eid[sizeof(ek.eid)-1] = 0;
    ecs_set_id(w->ecs, e, w->comp_EntityKey, sizeof(EntityKey), &ek);

    cy_entity_insert(w, eid, e);

    /* Persist to Redis: HASH + spatial ZSET */
    char ent_key_buf[256];
    snprintf(ent_key_buf, sizeof(ent_key_buf), "cy:ent:{%s:%s}:%s", wid, zone, eid);
    RedisModuleString *ent_key = RedisModule_CreateString(ctx, ent_key_buf, strlen(ent_key_buf));

    hset_str(ctx, ent_key, "eid",    eid);
    hset_str(ctx, ent_key, "zone",   zone);
    hset_str(ctx, ent_key, "world",  wid);
    hset_str(ctx, ent_key, "type",   type);
    hset_double(ctx, ent_key, "x",   x);
    hset_double(ctx, ent_key, "y",   y);
    hset_double(ctx, ent_key, "vx",  vx);
    hset_double(ctx, ent_key, "vy",  vy);
    hset_int(ctx, ent_key,    "hp",  hp);
    hset_int(ctx, ent_key,    "max_hp", max_hp);

    /* Spatial ZSET */
    char spatial_key_buf[256];
    snprintf(spatial_key_buf, sizeof(spatial_key_buf), "cy:spatial:{%s:%s}", wid, zone);
    char score_buf[32];
    snprintf(score_buf, sizeof(score_buf), "%lld", spatial_score(x, y));
    RedisModuleCallReply *r = RedisModule_Call(ctx, "ZADD", "ccc",
        spatial_key_buf, score_buf, eid);
    if (r) RedisModule_FreeCallReply(r);

    RedisModule_FreeString(ctx, ent_key);
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── FLECS.DELETE world eid ──────────────────────────────────────────────── */
static int Cmd_FLECS_DELETE(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    size_t wlen, elen;
    const char *wid = RedisModule_StringPtrLen(argv[1], &wlen);
    const char *eid = RedisModule_StringPtrLen(argv[2], &elen);

    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");

    ecs_entity_t e = cy_entity_lookup(w, eid);
    if (!e) return RedisModule_ReplyWithLongLong(ctx, 0);

    /* Need zone to clean up spatial ZSET */
    const ZoneId *zi = (const ZoneId *)ecs_get_id(w->ecs, e, w->comp_ZoneId);
    if (zi) {
        char spatial_key_buf[256];
        snprintf(spatial_key_buf, sizeof(spatial_key_buf), "cy:spatial:{%s:%s}", wid, zi->zone);
        RedisModuleCallReply *r = RedisModule_Call(ctx, "ZREM", "cc", spatial_key_buf, eid);
        if (r) RedisModule_FreeCallReply(r);

        char ent_key_buf[256];
        snprintf(ent_key_buf, sizeof(ent_key_buf), "cy:ent:{%s:%s}:%s", wid, zi->zone, eid);
        r = RedisModule_Call(ctx, "DEL", "c", ent_key_buf);
        if (r) RedisModule_FreeCallReply(r);
    }

    ecs_delete(w->ecs, e);
    cy_entity_erase(w, eid);
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── FLECS.GETCOMP world eid component ───────────────────────────────────── */
static int Cmd_FLECS_GETCOMP(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    size_t wlen, elen, clen;
    const char *wid  = RedisModule_StringPtrLen(argv[1], &wlen);
    const char *eid  = RedisModule_StringPtrLen(argv[2], &elen);
    const char *comp = RedisModule_StringPtrLen(argv[3], &clen);

    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");
    ecs_entity_t e = cy_entity_lookup(w, eid);
    if (!e) return RedisModule_ReplyWithError(ctx, "ERR entity not found");

    char xbuf[32], ybuf[32];
    if (strcmp(comp, "Position") == 0) {
        const Position *p = (const Position *)ecs_get_id(w->ecs, e, w->comp_Position);
        if (!p) { RedisModule_ReplyWithArray(ctx, 0); return REDISMODULE_OK; }
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithSimpleString(ctx, "x");
        snprintf(xbuf, sizeof(xbuf), "%.6f", p->x);
        RedisModule_ReplyWithSimpleString(ctx, xbuf);
        RedisModule_ReplyWithSimpleString(ctx, "y");
        snprintf(ybuf, sizeof(ybuf), "%.6f", p->y);
        RedisModule_ReplyWithSimpleString(ctx, ybuf);
    } else if (strcmp(comp, "Velocity") == 0) {
        const Velocity *v = (const Velocity *)ecs_get_id(w->ecs, e, w->comp_Velocity);
        if (!v) { RedisModule_ReplyWithArray(ctx, 0); return REDISMODULE_OK; }
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithSimpleString(ctx, "vx");
        snprintf(xbuf, sizeof(xbuf), "%.6f", v->vx);
        RedisModule_ReplyWithSimpleString(ctx, xbuf);
        RedisModule_ReplyWithSimpleString(ctx, "vy");
        snprintf(ybuf, sizeof(ybuf), "%.6f", v->vy);
        RedisModule_ReplyWithSimpleString(ctx, ybuf);
    } else if (strcmp(comp, "Health") == 0) {
        const Health *h = (const Health *)ecs_get_id(w->ecs, e, w->comp_Health);
        if (!h) { RedisModule_ReplyWithArray(ctx, 0); return REDISMODULE_OK; }
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithSimpleString(ctx, "hp");
        RedisModule_ReplyWithLongLong(ctx, h->hp);
        RedisModule_ReplyWithSimpleString(ctx, "max_hp");
        RedisModule_ReplyWithLongLong(ctx, h->max_hp);
    } else if (strcmp(comp, "ZoneId") == 0) {
        const ZoneId *z = (const ZoneId *)ecs_get_id(w->ecs, e, w->comp_ZoneId);
        if (!z) { RedisModule_ReplyWithArray(ctx, 0); return REDISMODULE_OK; }
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithSimpleString(ctx, "zone");
        RedisModule_ReplyWithSimpleString(ctx, z->zone);
    } else {
        return RedisModule_ReplyWithError(ctx, "ERR unknown component (Position|Velocity|Health|ZoneId)");
    }
    return REDISMODULE_OK;
}

/* ── FLECS.SETCOMP world eid component field value [field value ...] ──────── */
static int Cmd_FLECS_SETCOMP(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    /* argc must be >= 6 and even number of field/value pairs after component */
    if (argc < 6 || (argc % 2) != 0)
        return RedisModule_WrongArity(ctx);

    size_t wlen, elen, clen;
    const char *wid  = RedisModule_StringPtrLen(argv[1], &wlen);
    const char *eid  = RedisModule_StringPtrLen(argv[2], &elen);
    const char *comp = RedisModule_StringPtrLen(argv[3], &clen);

    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");
    ecs_entity_t e = cy_entity_lookup(w, eid);
    if (!e) return RedisModule_ReplyWithError(ctx, "ERR entity not found");

    if (strcmp(comp, "Position") == 0) {
        Position pos = {0, 0};
        const Position *cur = (const Position *)ecs_get_id(w->ecs, e, w->comp_Position);
        if (cur) pos = *cur;
        for (int i = 4; i < argc - 1; i += 2) {
            size_t flen; const char *f = RedisModule_StringPtrLen(argv[i], &flen);
            double v; RedisModule_StringToDouble(argv[i+1], &v);
            if (strcmp(f, "x") == 0) pos.x = v;
            else if (strcmp(f, "y") == 0) pos.y = v;
        }
        ecs_set_id(w->ecs, e, w->comp_Position, sizeof(Position), &pos);
    } else if (strcmp(comp, "Velocity") == 0) {
        Velocity vel = {0, 0};
        const Velocity *cur = (const Velocity *)ecs_get_id(w->ecs, e, w->comp_Velocity);
        if (cur) vel = *cur;
        for (int i = 4; i < argc - 1; i += 2) {
            size_t flen; const char *f = RedisModule_StringPtrLen(argv[i], &flen);
            double v; RedisModule_StringToDouble(argv[i+1], &v);
            if (strcmp(f, "vx") == 0) vel.vx = v;
            else if (strcmp(f, "vy") == 0) vel.vy = v;
        }
        ecs_set_id(w->ecs, e, w->comp_Velocity, sizeof(Velocity), &vel);
    } else if (strcmp(comp, "Health") == 0) {
        Health h = {100, 100};
        const Health *cur = (const Health *)ecs_get_id(w->ecs, e, w->comp_Health);
        if (cur) h = *cur;
        for (int i = 4; i < argc - 1; i += 2) {
            size_t flen; const char *f = RedisModule_StringPtrLen(argv[i], &flen);
            long long v; RedisModule_StringToLongLong(argv[i+1], &v);
            if (strcmp(f, "hp") == 0) h.hp = (int)v;
            else if (strcmp(f, "max_hp") == 0) h.max_hp = (int)v;
        }
        ecs_set_id(w->ecs, e, w->comp_Health, sizeof(Health), &h);
    } else {
        return RedisModule_ReplyWithError(ctx, "ERR unknown component (Position|Velocity|Health)");
    }
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── FLECS.HAS world eid component ───────────────────────────────────────── */
static int Cmd_FLECS_HAS(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    size_t wlen, elen, clen;
    const char *wid  = RedisModule_StringPtrLen(argv[1], &wlen);
    const char *eid  = RedisModule_StringPtrLen(argv[2], &elen);
    const char *comp = RedisModule_StringPtrLen(argv[3], &clen);

    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");
    ecs_entity_t e = cy_entity_lookup(w, eid);
    if (!e) return RedisModule_ReplyWithLongLong(ctx, 0);

    ecs_entity_t cid = 0;
    if      (strcmp(comp, "Position")  == 0) cid = w->comp_Position;
    else if (strcmp(comp, "Velocity")  == 0) cid = w->comp_Velocity;
    else if (strcmp(comp, "Health")    == 0) cid = w->comp_Health;
    else if (strcmp(comp, "ZoneId")    == 0) cid = w->comp_ZoneId;
    else if (strcmp(comp, "EntityKey") == 0) cid = w->comp_EntityKey;

    if (!cid) return RedisModule_ReplyWithLongLong(ctx, 0);
    return RedisModule_ReplyWithLongLong(ctx, ecs_has_id(w->ecs, e, cid) ? 1 : 0);
}

/* ── FLECS.TICK world zone dt_ms [budget] ────────────────────────────────── */
static int Cmd_FLECS_TICK(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4 || argc > 5) return RedisModule_WrongArity(ctx);

    size_t wlen, zlen;
    const char *wid  = RedisModule_StringPtrLen(argv[1], &wlen);
    const char *zone = RedisModule_StringPtrLen(argv[2], &zlen);
    long long dt_ms, budget = 256;

    if (RedisModule_StringToLongLong(argv[3], &dt_ms) != REDISMODULE_OK || dt_ms <= 0)
        return RedisModule_ReplyWithError(ctx, "ERR invalid dt_ms");
    if (argc == 5)
        RedisModule_StringToLongLong(argv[4], &budget);
    if (budget < 1 || budget > 4096) budget = 256;

    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");

    double dt = (double)dt_ms / 1000.0;

    /* ── 1. Drain intents stream ─────────────────────────────────────────── */
    char intents_key[256];
    snprintf(intents_key, sizeof(intents_key), "cy:intents:{%s:%s}", wid, zone);
    char budget_str[16];
    snprintf(budget_str, sizeof(budget_str), "%lld", budget);
    /* XRANGE cy:intents:{w:z} - + COUNT budget */
    RedisModuleCallReply *xr = RedisModule_Call(ctx, "XRANGE", "cccc",
        intents_key, "-", "+", budget_str);
    long long intents_processed = 0;
    if (xr && RedisModule_CallReplyType(xr) == REDISMODULE_REPLY_ARRAY) {
        size_t nm = RedisModule_CallReplyLength(xr);
        for (size_t m = 0; m < nm; m++) {
            RedisModuleCallReply *msg = RedisModule_CallReplyArrayElement(xr, m);
            if (!msg) continue;
            /* msg = [id, [field, val, ...]] */
            RedisModuleCallReply *id_rep  = RedisModule_CallReplyArrayElement(msg, 0);
            RedisModuleCallReply *fv_rep  = RedisModule_CallReplyArrayElement(msg, 1);
            if (!id_rep || !fv_rep) continue;

            size_t idlen;
            const char *msg_id = RedisModule_CallReplyStringBuffer(id_rep, &idlen);

            /* Parse intent: expect fields: eid, action, [dx, dy, target, ...] */
            size_t nfv = RedisModule_CallReplyLength(fv_rep);
            char eid_buf[128] = {0}, action_buf[64] = {0};
            double dx = 0, dy = 0;
            long long dmg = 0;
            for (size_t fi = 0; fi + 1 < nfv; fi += 2) {
                RedisModuleCallReply *fk = RedisModule_CallReplyArrayElement(fv_rep, fi);
                RedisModuleCallReply *fv = RedisModule_CallReplyArrayElement(fv_rep, fi + 1);
                if (!fk || !fv) continue;
                size_t kl, vl;
                const char *k = RedisModule_CallReplyStringBuffer(fk, &kl);
                const char *v = RedisModule_CallReplyStringBuffer(fv, &vl);
                if (!k || !v) continue;
                if      (strncmp(k, "eid",    kl) == 0 && kl == 3) { size_t cp = vl < 127 ? vl : 127; memcpy(eid_buf, v, cp); eid_buf[cp] = 0; }
                else if (strncmp(k, "action", kl) == 0 && kl == 6) { size_t cp = vl < 63  ? vl : 63;  memcpy(action_buf, v, cp); action_buf[cp] = 0; }
                else if (strncmp(k, "dx",     kl) == 0 && kl == 2) { char tb[32]; if(vl<32){memcpy(tb,v,vl);tb[vl]=0;dx=strtod(tb,NULL);} }
                else if (strncmp(k, "dy",     kl) == 0 && kl == 2) { char tb[32]; if(vl<32){memcpy(tb,v,vl);tb[vl]=0;dy=strtod(tb,NULL);} }
                else if (strncmp(k, "dmg",    kl) == 0 && kl == 3) { char tb[16]; if(vl<16){memcpy(tb,v,vl);tb[vl]=0;dmg=strtoll(tb,NULL,10);} }
            }

            if (eid_buf[0]) {
                ecs_entity_t e = cy_entity_lookup(w, eid_buf);
                if (e) {
                    if (strcmp(action_buf, "move") == 0) {
                        const Position *p = (const Position *)ecs_get_id(w->ecs, e, w->comp_Position);
                        if (p) {
                            Position np = { p->x + dx, p->y + dy };
                            ecs_set_id(w->ecs, e, w->comp_Position, sizeof(Position), &np);
                        }
                    } else if (strcmp(action_buf, "damage") == 0 && dmg > 0) {
                        const Health *h = (const Health *)ecs_get_id(w->ecs, e, w->comp_Health);
                        if (h) {
                            Health nh = { h->hp - (int)dmg, h->max_hp };
                            if (nh.hp < 0) nh.hp = 0;
                            ecs_set_id(w->ecs, e, w->comp_Health, sizeof(Health), &nh);
                        }
                    } else if (strcmp(action_buf, "set_vel") == 0) {
                        Velocity nv = { dx, dy };
                        ecs_set_id(w->ecs, e, w->comp_Velocity, sizeof(Velocity), &nv);
                    }
                }
            }

            /* XDEL after processing */
            if (msg_id) {
                char id_copy[64]; size_t cp = idlen < 63 ? idlen : 63;
                memcpy(id_copy, msg_id, cp); id_copy[cp] = 0;
                RedisModuleCallReply *dr = RedisModule_Call(ctx, "XDEL", "cc", intents_key, id_copy);
                if (dr) RedisModule_FreeCallReply(dr);
            }
            intents_processed++;
        }
        RedisModule_FreeCallReply(xr);
    } else if (xr) {
        RedisModule_FreeCallReply(xr);
    }

    /* ── 2. FLECS movement tick ──────────────────────────────────────────── */
    long long entities_processed = 0;
    long long events_emitted = 0;

    char events_key[256];
    snprintf(events_key, sizeof(events_key), "cy:events:{%s:%s}", wid, zone);
    char spatial_key[256];
    snprintf(spatial_key, sizeof(spatial_key), "cy:spatial:{%s:%s}", wid, zone);

    if (w->move_query) {
        ecs_iter_t it = ecs_query_iter(w->ecs, w->move_query);
        while (ecs_query_next(&it)) {
            Position  *positions  = ecs_field(&it, Position,  0);
            Velocity  *velocities = ecs_field(&it, Velocity,  1);
            ZoneId    *zones      = ecs_field(&it, ZoneId,    2);
            EntityKey *keys       = NULL;
            /* EntityKey may not be in the query terms — look it up per entity */

            for (int i = 0; i < it.count; i++) {
                /* Only process entities in the requested zone */
                if (strcmp(zones[i].zone, zone) != 0) continue;

                double old_x = positions[i].x, old_y = positions[i].y;
                positions[i].x += velocities[i].vx * dt;
                positions[i].y += velocities[i].vy * dt;

                /* Sync position back to Redis */
                const EntityKey *ek = (const EntityKey *)ecs_get_id(w->ecs, it.entities[i], w->comp_EntityKey);
                const char *eid = ek ? ek->eid : NULL;
                if (eid) {
                    char ent_key_buf[256];
                    snprintf(ent_key_buf, sizeof(ent_key_buf), "cy:ent:{%s:%s}:%s", wid, zone, eid);

                    RedisModuleString *ek_str = RedisModule_CreateString(ctx, ent_key_buf, strlen(ent_key_buf));
                    hset_double(ctx, ek_str, "x", positions[i].x);
                    hset_double(ctx, ek_str, "y", positions[i].y);
                    RedisModule_FreeString(ctx, ek_str);

                    /* Update spatial ZSET */
                    char score_buf[32];
                    snprintf(score_buf, sizeof(score_buf), "%lld", spatial_score(positions[i].x, positions[i].y));
                    RedisModuleCallReply *zr = RedisModule_Call(ctx, "ZADD", "ccc", spatial_key, score_buf, eid);
                    if (zr) RedisModule_FreeCallReply(zr);
                }
                entities_processed++;
            }
            (void)keys;
        }
    }

    /* ── 3. Increment tick counter ───────────────────────────────────────── */
    char tick_key[256];
    snprintf(tick_key, sizeof(tick_key), "cy:tick:{%s:%s}", wid, zone);
    RedisModuleCallReply *tr = RedisModule_Call(ctx, "HINCRBY", "ccl", tick_key, "tick", (long long)1);
    long long tick_num = 0;
    if (tr) {
        if (RedisModule_CallReplyType(tr) == REDISMODULE_REPLY_INTEGER)
            tick_num = RedisModule_CallReplyInteger(tr);
        RedisModule_FreeCallReply(tr);
    }

    /* Reply: [tick_num, entities_processed, events_emitted] */
    RedisModule_ReplyWithArray(ctx, 6);
    RedisModule_ReplyWithSimpleString(ctx, "tick");
    RedisModule_ReplyWithLongLong(ctx, tick_num);
    RedisModule_ReplyWithSimpleString(ctx, "entities");
    RedisModule_ReplyWithLongLong(ctx, entities_processed);
    RedisModule_ReplyWithSimpleString(ctx, "intents");
    RedisModule_ReplyWithLongLong(ctx, intents_processed);
    return REDISMODULE_OK;
}

/* ── FLECS.QUERY world filter [zone] ─────────────────────────────────────── */
static int Cmd_FLECS_QUERY(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);
    size_t wlen, flen;
    const char *wid    = RedisModule_StringPtrLen(argv[1], &wlen);
    const char *filter = RedisModule_StringPtrLen(argv[2], &flen);
    const char *zone_filter = NULL;
    if (argc == 4) {
        size_t zlen; zone_filter = RedisModule_StringPtrLen(argv[3], &zlen);
    }

    CyWorld *w = cy_world_get(wid);
    if (!w) return RedisModule_ReplyWithError(ctx, "ERR world not found");

    /* Parse simple filter: comma-separated component names */
    /* Build a query with up to 8 terms */
    ecs_query_desc_t qdesc = {0};
    int nterms = 0;
    char fbuf[256];
    strncpy(fbuf, filter, sizeof(fbuf) - 1);
    char *tok = strtok(fbuf, ",");
    while (tok && nterms < 8) {
        /* trim spaces */
        while (*tok == ' ') tok++;
        char *end = tok + strlen(tok) - 1;
        while (end > tok && *end == ' ') { *end = 0; end--; }

        ecs_entity_t cid = 0;
        if      (strcmp(tok, "Position")  == 0) cid = w->comp_Position;
        else if (strcmp(tok, "Velocity")  == 0) cid = w->comp_Velocity;
        else if (strcmp(tok, "Health")    == 0) cid = w->comp_Health;
        else if (strcmp(tok, "ZoneId")    == 0) cid = w->comp_ZoneId;
        else if (strcmp(tok, "EntityKey") == 0) cid = w->comp_EntityKey;

        if (cid) {
            qdesc.terms[nterms].id = cid;
            nterms++;
        }
        tok = strtok(NULL, ",");
    }

    /* Always include EntityKey so we can return string IDs */
    int has_ek = 0;
    for (int i = 0; i < nterms; i++)
        if (qdesc.terms[i].id == w->comp_EntityKey) { has_ek = 1; break; }
    if (!has_ek && nterms < 8) {
        qdesc.terms[nterms].id = w->comp_EntityKey;
        nterms++;
    }

    ecs_query_t *q = ecs_query_init(w->ecs, &qdesc);
    if (!q) { RedisModule_ReplyWithArray(ctx, 0); return REDISMODULE_OK; }

    /* Collect results */
    char *results[4096];
    int nresults = 0;

    ecs_iter_t it = ecs_query_iter(w->ecs, q);
    while (ecs_query_next(&it)) {
        /* Find EntityKey field index */
        int ek_term = -1;
        for (int t = 0; t < it.field_count; t++) {
            if (ecs_field_id(&it, t) == w->comp_EntityKey) { ek_term = t; break; }
        }
        /* Find ZoneId field index (optional) */
        int zone_term = -1;
        if (zone_filter) {
            for (int t = 0; t < it.field_count; t++) {
                if (ecs_field_id(&it, t) == w->comp_ZoneId) { zone_term = t; break; }
            }
        }

        for (int i = 0; i < it.count && nresults < 4096; i++) {
            if (zone_filter && zone_term >= 0) {
                const ZoneId *zi = (const ZoneId *)ecs_field_at(&it, zone_term, i);
                if (!zi || strcmp(zi->zone, zone_filter) != 0) continue;
            }
            if (ek_term >= 0) {
                const EntityKey *ek = (const EntityKey *)ecs_field_at(&it, ek_term, i);
                if (ek && ek->eid[0]) {
                    results[nresults++] = (char *)ek->eid;
                }
            }
        }
    }
    ecs_query_fini(q);

    RedisModule_ReplyWithArray(ctx, nresults);
    for (int i = 0; i < nresults; i++)
        RedisModule_ReplyWithCString(ctx, results[i]);
    return REDISMODULE_OK;
}

/* ── FLECS.RESTORE world_id ──────────────────────────────────────────────── */
static int Cmd_FLECS_RESTORE(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    size_t wlen;
    const char *wid = RedisModule_StringPtrLen(argv[1], &wlen);

    CyWorld *w = cy_world_get(wid);
    if (!w) {
        w = cy_world_create(ctx, wid);
        if (!w) return RedisModule_ReplyWithError(ctx, "ERR world creation failed");
    }

    /* SCAN for keys matching cy:ent:{world:*}:* */
    char pattern[256];
    snprintf(pattern, sizeof(pattern), "cy:ent:{%s:*}:*", wid);

    long long cursor = 0;
    long long restored = 0;
    do {
        char cursor_str[32];
        snprintf(cursor_str, sizeof(cursor_str), "%lld", cursor);
        RedisModuleCallReply *scan = RedisModule_Call(ctx, "SCAN", "cccc",
            cursor_str, "MATCH", pattern, "COUNT", "100");
        if (!scan) break;

        RedisModuleCallReply *cursor_rep = RedisModule_CallReplyArrayElement(scan, 0);
        RedisModuleCallReply *keys_rep   = RedisModule_CallReplyArrayElement(scan, 1);
        if (!cursor_rep || !keys_rep) { RedisModule_FreeCallReply(scan); break; }

        size_t clen;
        const char *cs = RedisModule_CallReplyStringBuffer(cursor_rep, &clen);
        char cbuf[32]; if (clen < 32) { memcpy(cbuf, cs, clen); cbuf[clen] = 0; }
        cursor = strtoll(cbuf, NULL, 10);

        size_t nkeys = RedisModule_CallReplyLength(keys_rep);
        for (size_t ki = 0; ki < nkeys; ki++) {
            RedisModuleCallReply *key_rep = RedisModule_CallReplyArrayElement(keys_rep, ki);
            if (!key_rep) continue;
            size_t kl;
            const char *key_str = RedisModule_CallReplyStringBuffer(key_rep, &kl);
            if (!key_str) continue;

            /* HGETALL the entity hash */
            RedisModuleCallReply *hga = RedisModule_Call(ctx, "HGETALL", "b", key_str, kl);
            if (!hga) continue;

            char eid_buf[128] = {0}, zone_buf[64] = {0}, type_buf[64] = {0};
            double x = 0, y = 0, vx = 0, vy = 0;
            int hp = 100, max_hp = 100;

            size_t nfv = RedisModule_CallReplyLength(hga);
            for (size_t fi = 0; fi + 1 < nfv; fi += 2) {
                RedisModuleCallReply *fk = RedisModule_CallReplyArrayElement(hga, fi);
                RedisModuleCallReply *fv = RedisModule_CallReplyArrayElement(hga, fi + 1);
                if (!fk || !fv) continue;
                size_t fl, vl2;
                const char *f = RedisModule_CallReplyStringBuffer(fk, &fl);
                const char *v = RedisModule_CallReplyStringBuffer(fv, &vl2);
                if (!f || !v) continue;
                char vbuf[128]; size_t cp = vl2 < 127 ? vl2 : 127;
                memcpy(vbuf, v, cp); vbuf[cp] = 0;

                if      (fl == 3 && strncmp(f, "eid",    3) == 0) { memcpy(eid_buf,  vbuf, cp < 127 ? cp : 127); }
                else if (fl == 4 && strncmp(f, "zone",   4) == 0) { memcpy(zone_buf, vbuf, cp < 63  ? cp : 63);  }
                else if (fl == 4 && strncmp(f, "type",   4) == 0) { memcpy(type_buf, vbuf, cp < 63  ? cp : 63);  }
                else if (fl == 1 && strncmp(f, "x",      1) == 0) x  = strtod(vbuf, NULL);
                else if (fl == 1 && strncmp(f, "y",      1) == 0) y  = strtod(vbuf, NULL);
                else if (fl == 2 && strncmp(f, "vx",     2) == 0) vx = strtod(vbuf, NULL);
                else if (fl == 2 && strncmp(f, "vy",     2) == 0) vy = strtod(vbuf, NULL);
                else if (fl == 2 && strncmp(f, "hp",     2) == 0) hp = (int)strtol(vbuf, NULL, 10);
                else if (fl == 6 && strncmp(f, "max_hp", 6) == 0) max_hp = (int)strtol(vbuf, NULL, 10);
            }
            RedisModule_FreeCallReply(hga);

            if (!eid_buf[0] || !zone_buf[0]) continue;
            if (cy_entity_lookup(w, eid_buf) != 0) continue;  /* skip duplicates */

            ecs_entity_t e = ecs_new(w->ecs);
            Position pos = { x, y };
            ecs_set_id(w->ecs, e, w->comp_Position, sizeof(Position), &pos);
            Velocity vel = { vx, vy };
            ecs_set_id(w->ecs, e, w->comp_Velocity, sizeof(Velocity), &vel);
            Health   h2  = { hp, max_hp };
            ecs_set_id(w->ecs, e, w->comp_Health, sizeof(Health), &h2);
            ZoneId   zi; strncpy(zi.zone, zone_buf, sizeof(zi.zone)-1); zi.zone[sizeof(zi.zone)-1]=0;
            ecs_set_id(w->ecs, e, w->comp_ZoneId, sizeof(ZoneId), &zi);
            EntityKey ek; strncpy(ek.eid, eid_buf, sizeof(ek.eid)-1); ek.eid[sizeof(ek.eid)-1]=0;
            ecs_set_id(w->ecs, e, w->comp_EntityKey, sizeof(EntityKey), &ek);

            cy_entity_insert(w, eid_buf, e);
            restored++;
        }
        RedisModule_FreeCallReply(scan);
    } while (cursor != 0);

    RedisModule_Log(ctx, "notice", "cy_game: restored %lld entities into world '%s'", restored, wid);
    return RedisModule_ReplyWithLongLong(ctx, restored);
}

/* ── Module entry point ──────────────────────────────────────────────────── */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    (void)argv; (void)argc;

    if (RedisModule_Init(ctx, "cy_game", 1, REDISMODULE_APIVER_1) != REDISMODULE_OK)
        return REDISMODULE_ERR;

    /* Wire FLECS allocator to Redis Module allocator */
    ecs_os_api_t api = ecs_os_get_api();
    api.malloc_  = cy_malloc;
    api.realloc_ = cy_realloc;
    api.free_    = cy_free;
    api.calloc_  = cy_calloc;
    ecs_os_set_api(&api);

    memset(g_worlds, 0, sizeof(g_worlds));

    /* Register FLECS.* commands */
#define REG(name, fn, flags, k1, kl, ks) \
    if (RedisModule_CreateCommand(ctx, name, fn, flags, k1, kl, ks) != REDISMODULE_OK) return REDISMODULE_ERR

    REG("FLECS.INIT",    Cmd_FLECS_INIT,    "write fast",    0, 0, 0);
    REG("FLECS.FINI",    Cmd_FLECS_FINI,    "write fast",    0, 0, 0);
    REG("FLECS.STATS",   Cmd_FLECS_STATS,   "readonly fast", 0, 0, 0);
    REG("FLECS.SPAWN",   Cmd_FLECS_SPAWN,   "write fast",    0, 0, 0);
    REG("FLECS.DELETE",  Cmd_FLECS_DELETE,  "write fast",    0, 0, 0);
    REG("FLECS.GETCOMP", Cmd_FLECS_GETCOMP, "readonly fast", 0, 0, 0);
    REG("FLECS.SETCOMP", Cmd_FLECS_SETCOMP, "write fast",    0, 0, 0);
    REG("FLECS.HAS",     Cmd_FLECS_HAS,     "readonly fast", 0, 0, 0);
    REG("FLECS.TICK",    Cmd_FLECS_TICK,    "write fast",    0, 0, 0);
    REG("FLECS.QUERY",   Cmd_FLECS_QUERY,   "readonly fast", 0, 0, 0);
    REG("FLECS.RESTORE", Cmd_FLECS_RESTORE, "write fast",    0, 0, 0);
#undef REG

    /* Load sub-modules */
    if (PathfindingModule_Init(ctx) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (PhysicsModule_Init(ctx)    != REDISMODULE_OK) return REDISMODULE_ERR;
    if (GOAPModule_Init(ctx)       != REDISMODULE_OK) return REDISMODULE_ERR;

    RedisModule_Log(ctx, "notice", "cy_game module loaded: FLECS + CYPATH + CYPHYS + CYGOAP");
    return REDISMODULE_OK;
}
