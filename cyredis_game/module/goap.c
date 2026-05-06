/* goap.c — Goal-Oriented Action Planning as Redis module commands
 *
 * World state and actions stored in Redis HASHes:
 *   ws_key     HASH: fact_name → "0"|"1"
 *   actions_key HASH: action_name → JSON {"pre":{"f":v},"eff":{"f":v},"cost":N}
 *
 * Commands:
 *   CYGOAP.SETSTATE   ws_key fact val [fact val ...]
 *   CYGOAP.DEFACTION  actions_key name pre_json eff_json cost
 *   CYGOAP.PLAN       ws_key actions_key goal_fact goal_value [max_depth]
 *   CYGOAP.APPLY      ws_key actions_key action_name
 */
#include "goap.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Minimal JSON parser for {"key": true/false/N} ──────────────────────── */
#define GOAP_MAX_FACTS   64
#define GOAP_MAX_ACTIONS 128
#define GOAP_MAX_DEPTH   32

typedef struct {
    char key[64];
    int  value;   /* 0 or 1 */
} GFact;

typedef struct {
    char   name[64];
    GFact  pre[GOAP_MAX_FACTS];
    int    pre_count;
    GFact  eff[GOAP_MAX_FACTS];
    int    eff_count;
    double cost;
} GAction;

/* Parse simple flat JSON {"key": true/false/0/1, ...} into a GFact array.
 * Returns number of facts parsed. */
static int parse_fact_json(const char *json, size_t jlen, GFact *facts, int max_facts) {
    int count = 0;
    const char *p = json, *end = json + jlen;
    while (p < end && count < max_facts) {
        /* find next '"' (key start) */
        while (p < end && *p != '"') p++;
        if (p >= end) break;
        p++;  /* skip opening quote */
        const char *key_start = p;
        while (p < end && *p != '"') p++;
        if (p >= end) break;
        size_t klen = (size_t)(p - key_start);
        if (klen == 0 || klen >= sizeof(facts[count].key)) { p++; continue; }
        memcpy(facts[count].key, key_start, klen);
        facts[count].key[klen] = '\0';
        p++;  /* skip closing quote */

        /* find ':' */
        while (p < end && *p != ':') p++;
        if (p >= end) break;
        p++;

        /* skip whitespace */
        while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
        if (p >= end) break;

        if (*p == 't') { facts[count].value = 1; p += 4; }
        else if (*p == 'f') { facts[count].value = 0; p += 5; }
        else if (*p == '1') { facts[count].value = 1; p++; }
        else if (*p == '0') { facts[count].value = 0; p++; }
        else p++;

        count++;
    }
    return count;
}

/* ── CYGOAP.SETSTATE ws_key fact val [fact val ...] ─────────────────────── */
static int CyGOAP_SetState(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4 || (argc % 2) != 0) return RedisModule_WrongArity(ctx);
    for (int i = 2; i < argc; i += 2) {
        RedisModule_Call(ctx, "HSET", "sss", argv[1], argv[i], argv[i+1]);
    }
    return RedisModule_ReplyWithLongLong(ctx, (argc - 2) / 2);
}

/* ── CYGOAP.DEFACTION actions_key name pre_json eff_json cost ────────────── */
static int CyGOAP_DefAction(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 6) return RedisModule_WrongArity(ctx);
    /* Store as single JSON: {"pre":..., "eff":..., "cost":...} */
    size_t pre_len, eff_len, cost_len;
    const char *pre  = RedisModule_StringPtrLen(argv[3], &pre_len);
    const char *eff  = RedisModule_StringPtrLen(argv[4], &eff_len);
    const char *cost = RedisModule_StringPtrLen(argv[5], &cost_len);

    /* Compose combined JSON */
    char *combined = RedisModule_Alloc(pre_len + eff_len + cost_len + 64);
    int n = snprintf(combined, pre_len + eff_len + cost_len + 64,
                     "{\"pre\":%.*s,\"eff\":%.*s,\"cost\":%.*s}",
                     (int)pre_len, pre, (int)eff_len, eff, (int)cost_len, cost);
    RedisModuleString *val = RedisModule_CreateString(ctx, combined, (size_t)n);
    RedisModule_Free(combined);
    RedisModule_Call(ctx, "HSET", "sss", argv[1], argv[2], val);
    RedisModule_FreeString(ctx, val);
    return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* ── State representation for BFS planner ───────────────────────────────── */
typedef struct {
    GFact   facts[GOAP_MAX_FACTS];
    int     count;
    int     cost;            /* g-score */
    int     parent;          /* plan node index */
    int     action_idx;      /* which action led here (-1 for start) */
} PlanNode;

#define PLAN_NODES_MAX 1024

static int state_satisfies_goal(PlanNode *s, const char *goal_fact, int goal_val) {
    for (int i = 0; i < s->count; i++) {
        if (strcmp(s->facts[i].key, goal_fact) == 0)
            return s->facts[i].value == goal_val;
    }
    return 0;
}

static int can_apply(PlanNode *s, GAction *a) {
    for (int i = 0; i < a->pre_count; i++) {
        int found = 0;
        for (int j = 0; j < s->count; j++) {
            if (strcmp(s->facts[j].key, a->pre[i].key) == 0) {
                if (s->facts[j].value != a->pre[i].value) return 0;
                found = 1; break;
            }
        }
        if (!found && a->pre[i].value != 0) return 0;
    }
    return 1;
}

static void apply_effects(PlanNode *dst, PlanNode *src, GAction *a) {
    *dst = *src;
    for (int i = 0; i < a->eff_count; i++) {
        int found = 0;
        for (int j = 0; j < dst->count; j++) {
            if (strcmp(dst->facts[j].key, a->eff[i].key) == 0) {
                dst->facts[j].value = a->eff[i].value;
                found = 1; break;
            }
        }
        if (!found && dst->count < GOAP_MAX_FACTS) {
            dst->facts[dst->count++] = a->eff[i];
        }
    }
}

/* ── CYGOAP.PLAN ws_key actions_key goal_fact goal_value [max_depth] ─────── */
static int CyGOAP_Plan(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 5 || argc > 6) return RedisModule_WrongArity(ctx);

    long long max_depth = 10;
    if (argc == 6) RedisModule_StringToLongLong(argv[5], &max_depth);
    if (max_depth < 1 || max_depth > GOAP_MAX_DEPTH) max_depth = 10;

    size_t gfact_len, gval_len;
    const char *goal_fact = RedisModule_StringPtrLen(argv[3], &gfact_len);
    const char *goal_val_s = RedisModule_StringPtrLen(argv[4], &gval_len);
    int goal_val = (goal_val_s && (goal_val_s[0] == '1' || goal_val_s[0] == 't')) ? 1 : 0;

    /* Load world state */
    RedisModuleCallReply *ws_rep = RedisModule_Call(ctx, "HGETALL", "s", argv[1]);
    PlanNode *start_node = RedisModule_Calloc(1, sizeof(PlanNode));
    start_node->parent = -1;
    start_node->action_idx = -1;
    if (ws_rep && RedisModule_CallReplyType(ws_rep) == REDISMODULE_REPLY_ARRAY) {
        size_t n = RedisModule_CallReplyLength(ws_rep);
        for (size_t i = 0; i + 1 < n && start_node->count < GOAP_MAX_FACTS; i += 2) {
            RedisModuleCallReply *k = RedisModule_CallReplyArrayElement(ws_rep, i);
            RedisModuleCallReply *v = RedisModule_CallReplyArrayElement(ws_rep, i+1);
            size_t klen, vlen;
            const char *ks = RedisModule_CallReplyStringBuffer(k, &klen);
            const char *vs = RedisModule_CallReplyStringBuffer(v, &vlen);
            if (!ks || klen >= GOAP_MAX_FACTS) continue;
            memcpy(start_node->facts[start_node->count].key, ks, klen);
            start_node->facts[start_node->count].key[klen] = '\0';
            start_node->facts[start_node->count].value = (vs && (vs[0]=='1'||vs[0]=='t')) ? 1 : 0;
            start_node->count++;
        }
    }
    if (ws_rep) RedisModule_FreeCallReply(ws_rep);

    /* Check if goal already satisfied */
    if (state_satisfies_goal(start_node, goal_fact, goal_val)) {
        RedisModule_Free(start_node);
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    }

    /* Load actions */
    RedisModuleCallReply *act_rep = RedisModule_Call(ctx, "HGETALL", "s", argv[2]);
    GAction *actions = RedisModule_Calloc(GOAP_MAX_ACTIONS, sizeof(GAction));
    int action_count = 0;
    if (act_rep && RedisModule_CallReplyType(act_rep) == REDISMODULE_REPLY_ARRAY) {
        size_t n = RedisModule_CallReplyLength(act_rep);
        for (size_t i = 0; i + 1 < n && action_count < GOAP_MAX_ACTIONS; i += 2) {
            RedisModuleCallReply *k = RedisModule_CallReplyArrayElement(act_rep, i);
            RedisModuleCallReply *v = RedisModule_CallReplyArrayElement(act_rep, i+1);
            size_t klen, vlen;
            const char *ks = RedisModule_CallReplyStringBuffer(k, &klen);
            const char *vs = RedisModule_CallReplyStringBuffer(v, &vlen);
            if (!ks || !vs || klen >= 64) continue;
            GAction *a = &actions[action_count];
            memcpy(a->name, ks, klen); a->name[klen] = '\0';
            a->cost = 1.0;
            /* Parse combined JSON {"pre":{...},"eff":{...},"cost":N} */
            const char *pre_start = strstr(vs, "\"pre\":");
            const char *eff_start = strstr(vs, "\"eff\":");
            if (pre_start) {
                pre_start += 6; /* skip "pre": */
                while (*pre_start == ' ') pre_start++;
                if (*pre_start == '{') {
                    const char *pre_end = pre_start + 1;
                    while (*pre_end && *pre_end != '}') pre_end++;
                    a->pre_count = parse_fact_json(pre_start, (size_t)(pre_end - pre_start + 1), a->pre, GOAP_MAX_FACTS);
                }
            }
            if (eff_start) {
                eff_start += 6;
                while (*eff_start == ' ') eff_start++;
                if (*eff_start == '{') {
                    const char *eff_end = eff_start + 1;
                    while (*eff_end && *eff_end != '}') eff_end++;
                    a->eff_count = parse_fact_json(eff_start, (size_t)(eff_end - eff_start + 1), a->eff, GOAP_MAX_FACTS);
                }
            }
            const char *cost_start = strstr(vs, "\"cost\":");
            if (cost_start) a->cost = strtod(cost_start + 7, NULL);
            action_count++;
        }
    }
    if (act_rep) RedisModule_FreeCallReply(act_rep);

    /* BFS/greedy forward planner over state space */
    PlanNode *nodes = RedisModule_Calloc(PLAN_NODES_MAX, sizeof(PlanNode));
    nodes[0] = *start_node;
    RedisModule_Free(start_node);
    int node_count = 1;
    int goal_node = -1;

    /* Simple queue (BFS, bounded by max_depth) */
    int queue[PLAN_NODES_MAX];
    int q_head = 0, q_tail = 0;
    queue[q_tail++] = 0;

    while (q_head < q_tail && goal_node < 0) {
        int cur = queue[q_head++];
        if (nodes[cur].cost >= max_depth) continue;
        for (int ai = 0; ai < action_count && goal_node < 0; ai++) {
            if (!can_apply(&nodes[cur], &actions[ai])) continue;
            if (node_count >= PLAN_NODES_MAX) break;
            PlanNode *next = &nodes[node_count];
            apply_effects(next, &nodes[cur], &actions[ai]);
            next->cost = nodes[cur].cost + 1;
            next->parent = cur;
            next->action_idx = ai;
            if (state_satisfies_goal(next, goal_fact, goal_val)) { goal_node = node_count; break; }
            queue[q_tail % PLAN_NODES_MAX] = node_count;
            if (q_tail - q_head < PLAN_NODES_MAX) q_tail++;
            node_count++;
        }
    }

    if (goal_node < 0) {
        RedisModule_Free(nodes); RedisModule_Free(actions);
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    }

    /* Reconstruct plan */
    char *plan[GOAP_MAX_DEPTH];
    int plan_len = 0;
    int n = goal_node;
    while (n > 0 && plan_len < GOAP_MAX_DEPTH) {
        plan[plan_len++] = actions[nodes[n].action_idx].name;
        n = nodes[n].parent;
    }

    RedisModule_ReplyWithArray(ctx, plan_len);
    for (int i = plan_len - 1; i >= 0; i--)
        RedisModule_ReplyWithSimpleString(ctx, plan[i]);

    RedisModule_Free(nodes); RedisModule_Free(actions);
    return REDISMODULE_OK;
}

/* ── CYGOAP.APPLY ws_key actions_key action_name ────────────────────────── */
static int CyGOAP_Apply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4) return RedisModule_WrongArity(ctx);

    RedisModuleCallReply *rep = RedisModule_Call(ctx, "HGET", "ss", argv[2], argv[3]);
    if (!rep || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
        if (rep) RedisModule_FreeCallReply(rep);
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithSimpleString(ctx, "failed");
        RedisModule_ReplyWithSimpleString(ctx, "action_not_found");
        return REDISMODULE_OK;
    }

    size_t vlen;
    const char *vs = RedisModule_CallReplyStringBuffer(rep, &vlen);
    GAction a; memset(&a, 0, sizeof(a));
    /* Parse pre / eff from combined JSON */
    const char *pre_start = strstr(vs, "\"pre\":");
    const char *eff_start = strstr(vs, "\"eff\":");
    if (pre_start) {
        pre_start += 6;
        while (*pre_start == ' ') pre_start++;
        if (*pre_start == '{') {
            const char *pre_end = pre_start + 1; while (*pre_end && *pre_end != '}') pre_end++;
            a.pre_count = parse_fact_json(pre_start, (size_t)(pre_end - pre_start + 1), a.pre, GOAP_MAX_FACTS);
        }
    }
    if (eff_start) {
        eff_start += 6;
        while (*eff_start == ' ') eff_start++;
        if (*eff_start == '{') {
            const char *eff_end = eff_start + 1; while (*eff_end && *eff_end != '}') eff_end++;
            a.eff_count = parse_fact_json(eff_start, (size_t)(eff_end - eff_start + 1), a.eff, GOAP_MAX_FACTS);
        }
    }
    RedisModule_FreeCallReply(rep);

    /* Load current world state for precondition check */
    RedisModuleCallReply *ws_rep = RedisModule_Call(ctx, "HGETALL", "s", argv[1]);
    GFact ws_facts[GOAP_MAX_FACTS]; int ws_count = 0;
    if (ws_rep && RedisModule_CallReplyType(ws_rep) == REDISMODULE_REPLY_ARRAY) {
        size_t n = RedisModule_CallReplyLength(ws_rep);
        for (size_t i = 0; i+1 < n && ws_count < GOAP_MAX_FACTS; i += 2) {
            size_t klen, vl;
            const char *ks = RedisModule_CallReplyStringBuffer(RedisModule_CallReplyArrayElement(ws_rep, i), &klen);
            const char *vv = RedisModule_CallReplyStringBuffer(RedisModule_CallReplyArrayElement(ws_rep, i+1), &vl);
            if (!ks || klen >= 64) continue;
            memcpy(ws_facts[ws_count].key, ks, klen); ws_facts[ws_count].key[klen] = '\0';
            ws_facts[ws_count].value = (vv && (vv[0]=='1'||vv[0]=='t')) ? 1 : 0;
            ws_count++;
        }
    }
    if (ws_rep) RedisModule_FreeCallReply(ws_rep);

    /* Check preconditions */
    for (int i = 0; i < a.pre_count; i++) {
        int ok = 0;
        for (int j = 0; j < ws_count; j++) {
            if (strcmp(ws_facts[j].key, a.pre[i].key) == 0) {
                if (ws_facts[j].value != a.pre[i].value) {
                    RedisModule_ReplyWithArray(ctx, 2);
                    RedisModule_ReplyWithSimpleString(ctx, "failed");
                    RedisModule_ReplyWithSimpleString(ctx, a.pre[i].key);
                    return REDISMODULE_OK;
                }
                ok = 1; break;
            }
        }
        if (!ok && a.pre[i].value != 0) {
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithSimpleString(ctx, "failed");
            RedisModule_ReplyWithSimpleString(ctx, a.pre[i].key);
            return REDISMODULE_OK;
        }
    }

    /* Apply effects */
    for (int i = 0; i < a.eff_count; i++) {
        const char *v = a.eff[i].value ? "1" : "0";
        RedisModule_Call(ctx, "HSET", "scc", argv[1], a.eff[i].key, v);
    }

    RedisModule_ReplyWithArray(ctx, 1);
    RedisModule_ReplyWithSimpleString(ctx, "applied");
    return REDISMODULE_OK;
}

/* ── Module init ─────────────────────────────────────────────────────────── */
int GOAPModule_Init(RedisModuleCtx *ctx) {
    if (RedisModule_CreateCommand(ctx, "CYGOAP.SETSTATE",  CyGOAP_SetState,  "write fast",    1, 1, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYGOAP.DEFACTION", CyGOAP_DefAction, "write fast",    1, 1, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYGOAP.PLAN",      CyGOAP_Plan,      "readonly fast", 1, 2, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CYGOAP.APPLY",     CyGOAP_Apply,     "write fast",    1, 2, 1) != REDISMODULE_OK) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
