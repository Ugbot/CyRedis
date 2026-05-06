/* physics.c — CYPHYS.* Redis module commands
 *
 * Commands:
 *   CYPHYS.AABB    ax ay aw ah bx by bw bh      (center + half-extents)
 *   CYPHYS.SWEEP   ent_key obs_key vx vy dt cell_size
 *   CYPHYS.CIRCLE  spatial_zset_key cx cy radius [limit]
 *   CYPHYS.RESOLVE ent_a_key ent_b_key [min_sep]
 */
#include "physics.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ── Helpers ─────────────────────────────────────────────────────────────── */
static double reply_to_double(RedisModuleCallReply *rep, double def) {
    if (!rep || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) return def;
    if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_INTEGER)
        return (double)RedisModule_CallReplyInteger(rep);
    size_t len;
    const char *s = RedisModule_CallReplyStringBuffer(rep, &len);
    if (!s) return def;
    char buf[64];
    if (len >= sizeof(buf)) return def;
    memcpy(buf, s, len);
    buf[len] = '\0';
    return strtod(buf, NULL);
}

static double hget_double(RedisModuleCtx *ctx, RedisModuleString *key, const char *field, double def) {
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "HGET", "sc", key, field);
    double v = reply_to_double(rep, def);
    if (rep) RedisModule_FreeCallReply(rep);
    return v;
}

/* ── CYPHYS.AABB ax ay aw ah bx by bw bh ────────────────────────────────── */
static int CyPhys_AABB(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 9) return RedisModule_WrongArity(ctx);
    double ax, ay, aw, ah, bx, by, bw, bh;
    double vd;
    if (RedisModule_StringToDouble(argv[1], &ax) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[2], &ay) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[3], &aw) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[4], &ah) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[5], &bx) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[6], &by) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[7], &bw) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[8], &bh) != REDISMODULE_OK) {
        (void)vd;
        return RedisModule_ReplyWithError(ctx, "ERR invalid args");
    }
    /* Center+half-extent overlap test */
    int overlap = (fabs(ax - bx) < aw + bw) && (fabs(ay - by) < ah + bh);
    return RedisModule_ReplyWithLongLong(ctx, overlap);
}

/* ── CYPHYS.SWEEP ent_key obs_key vx vy dt cell_size ────────────────────── */
static int CyPhys_Sweep(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 7) return RedisModule_WrongArity(ctx);

    double vx, vy, dt, cell_size;
    if (RedisModule_StringToDouble(argv[3], &vx) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[4], &vy) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[5], &dt) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[6], &cell_size) != REDISMODULE_OK)
        return RedisModule_ReplyWithError(ctx, "ERR invalid args");
    if (cell_size <= 0) cell_size = 1.0;

    double ex = hget_double(ctx, argv[1], "x", 0.0);
    double ey = hget_double(ctx, argv[1], "y", 0.0);

    double move_x = vx * (dt / 1000.0);
    double move_y = vy * (dt / 1000.0);
    double nx = ex + move_x, ny = ey + move_y;

    /* Step along movement vector checking obstacle grid */
    int steps = (int)(fabs(move_x) / cell_size + fabs(move_y) / cell_size) + 2;
    if (steps > 256) steps = 256;
    double sx = move_x / steps, sy = move_y / steps;
    double cx = ex, cy = ey;
    int hit = 0;
    double hit_nx = 0, hit_ny = 0;

    RedisModuleKey *obs_key = RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ);

    for (int i = 1; i <= steps; i++) {
        cx += sx; cy += sy;
        int gx = (int)floor(cx / cell_size);
        int gy = (int)floor(cy / cell_size);
        char field[32];
        snprintf(field, sizeof(field), "%d,%d", gx, gy);
        RedisModuleString *fs = RedisModule_CreateString(ctx, field, strlen(field));
        RedisModuleString *val = NULL;
        if (obs_key) RedisModule_HashGet(obs_key, REDISMODULE_HASH_NONE, fs, &val, NULL);
        RedisModule_FreeString(ctx, fs);
        if (val) {
            RedisModule_FreeString(ctx, val);
            hit = 1;
            /* Normal: primary axis of movement */
            if (fabs(sx) >= fabs(sy)) { hit_nx = -1.0 * (sx > 0 ? 1 : -1); hit_ny = 0; }
            else                       { hit_nx = 0; hit_ny = -1.0 * (sy > 0 ? 1 : -1); }
            nx = cx - sx; ny = cy - sy;  /* back up one step */
            break;
        }
    }
    if (obs_key) RedisModule_CloseKey(obs_key);

    /* Reply: [new_x_str, new_y_str, hit, nx, ny] */
    RedisModule_ReplyWithArray(ctx, 5);
    char buf[32];
    snprintf(buf, sizeof(buf), "%.6f", nx);
    RedisModule_ReplyWithSimpleString(ctx, buf);
    snprintf(buf, sizeof(buf), "%.6f", ny);
    RedisModule_ReplyWithSimpleString(ctx, buf);
    RedisModule_ReplyWithLongLong(ctx, hit);
    snprintf(buf, sizeof(buf), "%.1f", hit_nx);
    RedisModule_ReplyWithSimpleString(ctx, buf);
    snprintf(buf, sizeof(buf), "%.1f", hit_ny);
    RedisModule_ReplyWithSimpleString(ctx, buf);
    return REDISMODULE_OK;
}

/* ── CYPHYS.CIRCLE spatial_key cx cy radius [limit] ─────────────────────── */
/* Spatial ZSET score encoding: floor(x*1000)*1e9 + floor(y*1000) */
static int CyPhys_Circle(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 5 || argc > 6) return RedisModule_WrongArity(ctx);

    double cx, cy, radius;
    long long limit = 50;
    if (RedisModule_StringToDouble(argv[2], &cx) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[3], &cy) != REDISMODULE_OK ||
        RedisModule_StringToDouble(argv[4], &radius) != REDISMODULE_OK)
        return RedisModule_ReplyWithError(ctx, "ERR invalid args");
    if (argc == 6) RedisModule_StringToLongLong(argv[5], &limit);

    /* Bounding box score range */
    long long x_min_score = (long long)((cx - radius) * 1000) * 1000000000LL;
    long long x_max_score = (long long)((cx + radius) * 1000) * 1000000000LL + 999999999LL;
    char smin[32], smax[32];
    snprintf(smin, sizeof(smin), "%lld", x_min_score);
    snprintf(smax, sizeof(smax), "%lld", x_max_score);
    char slim[32];
    snprintf(slim, sizeof(slim), "%lld", limit * 4);  /* oversample, filter below */

    RedisModuleCallReply *rep = RedisModule_Call(ctx, "ZRANGEBYSCORE", "scccc",
        argv[1], smin, smax, "LIMIT", "0");
    /* Note: ZRANGEBYSCORE LIMIT 0 N */
    if (!rep) { RedisModule_ReplyWithArray(ctx, 0); return REDISMODULE_OK; }

    typedef struct { char eid[128]; double dist; } Hit;
    Hit hits[512];
    int nhits = 0;

    if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY) {
        size_t n = RedisModule_CallReplyLength(rep);
        for (size_t i = 0; i < n && nhits < 512; i++) {
            RedisModuleCallReply *elem = RedisModule_CallReplyArrayElement(rep, i);
            if (!elem) continue;
            size_t elen;
            const char *estr = RedisModule_CallReplyStringBuffer(elem, &elen);
            /* To get actual position we need ZSCORE + decode. For simplicity,
             * use approximate distance from score encoding. */
            RedisModuleCallReply *sc = RedisModule_Call(ctx, "ZSCORE", "ss", argv[1],
                RedisModule_CreateStringFromCallReply(elem));
            if (!sc) continue;
            double score_d = 0;
            size_t slen;
            const char *ss = RedisModule_CallReplyStringBuffer(sc, &slen);
            if (ss) { char sbuf[32]; if (slen<32){memcpy(sbuf,ss,slen);sbuf[slen]=0;score_d=strtod(sbuf,NULL);} }
            RedisModule_FreeCallReply(sc);

            long long score = (long long)score_d;
            double ex = (double)(score / 1000000000LL) / 1000.0;
            double ey = (double)(score % 1000000000LL) / 1000.0;
            double dist = sqrt((ex-cx)*(ex-cx) + (ey-cy)*(ey-cy));
            if (dist > radius) continue;

            size_t copy_len = elen < 127 ? elen : 127;
            memcpy(hits[nhits].eid, estr, copy_len);
            hits[nhits].eid[copy_len] = '\0';
            hits[nhits].dist = dist;
            nhits++;
        }
    }
    RedisModule_FreeCallReply(rep);

    /* Sort by distance ascending (insertion sort, small n) */
    for (int i = 1; i < nhits; i++) {
        Hit tmp = hits[i]; int j = i - 1;
        while (j >= 0 && hits[j].dist > tmp.dist) { hits[j+1] = hits[j]; j--; }
        hits[j+1] = tmp;
    }
    if (nhits > limit) nhits = (int)limit;

    RedisModule_ReplyWithArray(ctx, nhits * 2);
    for (int i = 0; i < nhits; i++) {
        RedisModule_ReplyWithSimpleString(ctx, hits[i].eid);
        char dbuf[32];
        snprintf(dbuf, sizeof(dbuf), "%.4f", hits[i].dist);
        RedisModule_ReplyWithSimpleString(ctx, dbuf);
    }
    return REDISMODULE_OK;
}

/* ── CYPHYS.RESOLVE ent_a_key ent_b_key [min_sep] ───────────────────────── */
static int CyPhys_Resolve(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);

    double min_sep = 0.1;
    if (argc == 4) RedisModule_StringToDouble(argv[3], &min_sep);

    double ax = hget_double(ctx, argv[1], "x", 0);
    double ay = hget_double(ctx, argv[1], "y", 0);
    double aw = hget_double(ctx, argv[1], "w", 0.5);
    double ah = hget_double(ctx, argv[1], "h", 0.5);
    double bx = hget_double(ctx, argv[2], "x", 0);
    double by = hget_double(ctx, argv[2], "y", 0);
    double bw = hget_double(ctx, argv[2], "w", 0.5);
    double bh = hget_double(ctx, argv[2], "h", 0.5);

    double overlap_x = (aw + bw) - fabs(ax - bx);
    double overlap_y = (ah + bh) - fabs(ay - by);
    if (overlap_x <= 0 || overlap_y <= 0) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithSimpleString(ctx, "0");
        RedisModule_ReplyWithSimpleString(ctx, "0");
        return REDISMODULE_OK;
    }

    double dx = 0, dy = 0;
    if (overlap_x < overlap_y) dx = (ax < bx ? -1 : 1) * (overlap_x / 2 + min_sep);
    else                        dy = (ay < by ? -1 : 1) * (overlap_y / 2 + min_sep);

    char xbuf[32], ybuf[32];
    snprintf(xbuf, sizeof(xbuf), "%.6f", ax + dx);
    snprintf(ybuf, sizeof(ybuf), "%.6f", ay + dy);
    RedisModule_Call(ctx, "HSET", "scc", argv[1], "x", xbuf);
    RedisModule_Call(ctx, "HSET", "scc", argv[1], "y", ybuf);

    snprintf(xbuf, sizeof(xbuf), "%.6f", bx - dx);
    snprintf(ybuf, sizeof(ybuf), "%.6f", by - dy);
    RedisModule_Call(ctx, "HSET", "scc", argv[2], "x", xbuf);
    RedisModule_Call(ctx, "HSET", "scc", argv[2], "y", ybuf);

    char dbuf[32];
    snprintf(dbuf, sizeof(dbuf), "%.6f", fabs(dx) + fabs(dy));
    RedisModule_ReplyWithArray(ctx, 2);
    char dxbuf[32], dybuf[32];
    snprintf(dxbuf, sizeof(dxbuf), "%.6f", dx);
    snprintf(dybuf, sizeof(dybuf), "%.6f", dy);
    RedisModule_ReplyWithSimpleString(ctx, dxbuf);
    RedisModule_ReplyWithSimpleString(ctx, dybuf);
    return REDISMODULE_OK;
}

/* ── Module init ─────────────────────────────────────────────────────────── */
int PhysicsModule_Init(RedisModuleCtx *ctx) {
    if (RedisModule_CreateCommand(ctx, "CYPHYS.AABB",    CyPhys_AABB,    "readonly fast", 0, 0, 0) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYPHYS.SWEEP",   CyPhys_Sweep,   "write fast", 1, 2, 1)    != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYPHYS.CIRCLE",  CyPhys_Circle,  "readonly fast", 1, 1, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYPHYS.RESOLVE", CyPhys_Resolve, "write fast", 1, 2, 1)    != REDISMODULE_OK) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
