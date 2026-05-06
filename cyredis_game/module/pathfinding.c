/* pathfinding.c — A* pathfinding as Redis module commands
 *
 * Grid storage: sparse Redis HASH, field "x,y" present = blocked cell.
 * Absent field = passable.  Only stores walls, not empty space.
 *
 * Commands:
 *   CYPATH.SET   grid_key x y blocked(0|1)
 *   CYPATH.CLEAR grid_key
 *   CYPATH.FIND  grid_key sx sy gx gy [max_steps]
 */
#include "pathfinding.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ── A* data structures ─────────────────────────────────────────────────── */
#define ASTAR_MAX_NODES 16384
#define ASTAR_HEAP_MAX  ASTAR_MAX_NODES
#define VISITED_BUCKETS 8192

typedef struct {
    int x, y;
    int g, f;
    int parent;   /* index into node pool, -1 = no parent */
} ANode;

typedef struct {
    int  x, y;
    int  node_idx;
    int  used;
} VisitedEntry;

typedef struct {
    ANode nodes[ASTAR_MAX_NODES];
    int   node_count;
    int   heap[ASTAR_HEAP_MAX];
    int   heap_size;
    VisitedEntry visited[VISITED_BUCKETS];
} AStarCtx;

/* ── Heap (min by f) ─────────────────────────────────────────────────────── */
static void heap_push(AStarCtx *ctx, int idx) {
    if (ctx->heap_size >= ASTAR_HEAP_MAX) return;
    int pos = ctx->heap_size++;
    ctx->heap[pos] = idx;
    while (pos > 0) {
        int parent = (pos - 1) / 2;
        if (ctx->nodes[ctx->heap[parent]].f <= ctx->nodes[ctx->heap[pos]].f) break;
        int tmp = ctx->heap[parent]; ctx->heap[parent] = ctx->heap[pos]; ctx->heap[pos] = tmp;
        pos = parent;
    }
}

static int heap_pop(AStarCtx *ctx) {
    if (ctx->heap_size == 0) return -1;
    int top = ctx->heap[0];
    ctx->heap[0] = ctx->heap[--ctx->heap_size];
    int pos = 0;
    for (;;) {
        int l = 2*pos+1, r = 2*pos+2, best = pos;
        if (l < ctx->heap_size && ctx->nodes[ctx->heap[l]].f < ctx->nodes[ctx->heap[best]].f) best = l;
        if (r < ctx->heap_size && ctx->nodes[ctx->heap[r]].f < ctx->nodes[ctx->heap[best]].f) best = r;
        if (best == pos) break;
        int tmp = ctx->heap[pos]; ctx->heap[pos] = ctx->heap[best]; ctx->heap[best] = tmp;
        pos = best;
    }
    return top;
}

/* ── Visited set ─────────────────────────────────────────────────────────── */
static unsigned int vis_hash(int x, int y) {
    unsigned int h = (unsigned int)x * 2654435761u ^ (unsigned int)y * 2246822519u;
    return h & (VISITED_BUCKETS - 1);
}

static int vis_has(AStarCtx *ctx, int x, int y) {
    unsigned int h = vis_hash(x, y);
    for (int i = 0; i < VISITED_BUCKETS; i++) {
        int idx = (h + i) & (VISITED_BUCKETS - 1);
        if (!ctx->visited[idx].used) return 0;
        if (ctx->visited[idx].x == x && ctx->visited[idx].y == y) return 1;
    }
    return 0;
}

static int vis_get(AStarCtx *ctx, int x, int y) {
    unsigned int h = vis_hash(x, y);
    for (int i = 0; i < VISITED_BUCKETS; i++) {
        int idx = (h + i) & (VISITED_BUCKETS - 1);
        if (!ctx->visited[idx].used) return -1;
        if (ctx->visited[idx].x == x && ctx->visited[idx].y == y) return ctx->visited[idx].node_idx;
    }
    return -1;
}

static void vis_set(AStarCtx *ctx, int x, int y, int node_idx) {
    unsigned int h = vis_hash(x, y);
    for (int i = 0; i < VISITED_BUCKETS; i++) {
        int idx = (h + i) & (VISITED_BUCKETS - 1);
        if (!ctx->visited[idx].used || (ctx->visited[idx].x == x && ctx->visited[idx].y == y)) {
            ctx->visited[idx].x = x;
            ctx->visited[idx].y = y;
            ctx->visited[idx].node_idx = node_idx;
            ctx->visited[idx].used = 1;
            return;
        }
    }
}

/* ── Grid blocked-cell check via Redis HASH ─────────────────────────────── */
static int is_blocked(RedisModuleCtx *ctx, RedisModuleKey *grid_key, int x, int y) {
    if (!grid_key) return 0;
    char field[32];
    snprintf(field, sizeof(field), "%d,%d", x, y);
    RedisModuleString *field_str = RedisModule_CreateString(ctx, field, strlen(field));
    RedisModuleString *val = NULL;
    RedisModule_HashGet(grid_key, REDISMODULE_HASH_NONE, field_str, &val, NULL);
    RedisModule_FreeString(ctx, field_str);
    if (!val) return 0;
    size_t vlen;
    const char *vstr = RedisModule_StringPtrLen(val, &vlen);
    int blocked = (vstr && vstr[0] == '1');
    RedisModule_FreeString(ctx, val);
    return blocked;
}

/* ── CYPATH.SET grid_key x y blocked ────────────────────────────────────── */
static int CyPath_Set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5) return RedisModule_WrongArity(ctx);
    long long x, y, blocked;
    if (RedisModule_StringToLongLong(argv[2], &x) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[3], &y) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[4], &blocked) != REDISMODULE_OK)
        return RedisModule_ReplyWithError(ctx, "ERR invalid coordinates");

    char field[32];
    snprintf(field, sizeof(field), "%lld,%lld", x, y);

    if (blocked) {
        RedisModule_Call(ctx, "HSET", "scc", argv[1], field, "1");
    } else {
        RedisModule_Call(ctx, "HDEL", "sc", argv[1], field);
    }
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── CYPATH.CLEAR grid_key ───────────────────────────────────────────────── */
static int CyPath_Clear(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModule_Call(ctx, "DEL", "s", argv[1]);
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── CYPATH.FIND grid_key sx sy gx gy [max_steps] ───────────────────────── */
static int CyPath_Find(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 7 || argc > 8) return RedisModule_WrongArity(ctx);

    long long sx, sy, gx, gy, max_steps = 1024;
    if (RedisModule_StringToLongLong(argv[2], &sx) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[3], &sy) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[4], &gx) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[5], &gy) != REDISMODULE_OK)
        return RedisModule_ReplyWithError(ctx, "ERR invalid coordinates");
    if (argc == 8 && RedisModule_StringToLongLong(argv[7], &max_steps) != REDISMODULE_OK)
        return RedisModule_ReplyWithError(ctx, "ERR invalid max_steps");
    if (max_steps < 1 || max_steps > ASTAR_MAX_NODES) max_steps = 1024;

    /* Trivial case */
    if (sx == gx && sy == gy) {
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    }

    /* Open the grid HASH once for all lookups */
    RedisModuleKey *grid_key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    /* Allocate A* context (stack is fine for ~400KB) */
    AStarCtx *astar = RedisModule_Calloc(1, sizeof(AStarCtx));

    /* Seed start node */
    int abs_dx = (int)(gx - sx < 0 ? sx - gx : gx - sx);
    int abs_dy = (int)(gy - sy < 0 ? sy - gy : gy - sy);
    ANode start = {(int)sx, (int)sy, 0, abs_dx + abs_dy, -1};
    astar->nodes[0] = start;
    astar->node_count = 1;
    heap_push(astar, 0);

    /* Neighbour offsets: 4-directional */
    static const int dx[4] = {0, 0, -1, 1};
    static const int dy[4] = {-1, 1, 0, 0};

    int goal_node = -1;
    int steps = 0;

    while (astar->heap_size > 0 && steps < max_steps) {
        int cur_idx = heap_pop(astar);
        ANode *cur = &astar->nodes[cur_idx];
        steps++;

        if (cur->x == (int)gx && cur->y == (int)gy) { goal_node = cur_idx; break; }
        if (vis_has(astar, cur->x, cur->y)) continue;
        vis_set(astar, cur->x, cur->y, cur_idx);

        for (int d = 0; d < 4; d++) {
            int nx = cur->x + dx[d], ny = cur->y + dy[d];
            if (vis_has(astar, nx, ny)) continue;
            if (is_blocked(ctx, grid_key, nx, ny)) continue;

            int ng = cur->g + 1;
            int existing = vis_get(astar, nx, ny);
            if (existing >= 0 && astar->nodes[existing].g <= ng) continue;

            if (astar->node_count >= ASTAR_MAX_NODES) continue;
            int abs_ndx = (int)(gx - nx < 0 ? nx - gx : gx - nx);
            int abs_ndy = (int)(gy - ny < 0 ? ny - gy : gy - ny);
            ANode neighbour = {nx, ny, ng, ng + abs_ndx + abs_ndy, cur_idx};
            int ni = astar->node_count++;
            astar->nodes[ni] = neighbour;
            heap_push(astar, ni);
        }
    }

    if (grid_key) RedisModule_CloseKey(grid_key);

    if (goal_node < 0) {
        RedisModule_Free(astar);
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    }

    /* Reconstruct path (reversed) */
    int path[ASTAR_MAX_NODES * 2];
    int path_len = 0;
    int n = goal_node;
    while (n >= 0) {
        path[path_len++] = astar->nodes[n].y;
        path[path_len++] = astar->nodes[n].x;
        n = astar->nodes[n].parent;
    }
    /* Skip start node (path_len-2, path_len-1 are start coords) */
    int result_len = path_len - 2;
    RedisModule_Free(astar);

    RedisModule_ReplyWithArray(ctx, result_len);
    for (int i = result_len - 1; i >= 0; i -= 2) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", path[i - 1]);  /* x */
        RedisModule_ReplyWithSimpleString(ctx, buf);
        snprintf(buf, sizeof(buf), "%d", path[i]);       /* y */
        RedisModule_ReplyWithSimpleString(ctx, buf);
    }
    return REDISMODULE_OK;
}

/* ── Module init ─────────────────────────────────────────────────────────── */
int PathfindingModule_Init(RedisModuleCtx *ctx) {
    if (RedisModule_CreateCommand(ctx, "CYPATH.SET",   CyPath_Set,   "write fast", 1, 1, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYPATH.CLEAR", CyPath_Clear, "write fast", 1, 1, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYPATH.FIND",  CyPath_Find,  "readonly fast", 1, 1, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
