/* pgcache.c — PostgreSQL read-through / write-through cache Redis module
 *
 * Commands:
 *   PGCACHE.READ     table primary_key_json [ttl]
 *   PGCACHE.WRITE    table primary_key_json data_json [ttl]
 *   PGCACHE.INVALIDATE table primary_key_json
 *   PGCACHE.MULTIREAD  table primary_keys_json_array [ttl]
 *
 * Module args (MODULE LOAD pgcache.so key value ...):
 *   pg_host, pg_port, pg_database, pg_user, pg_password
 *   default_ttl, cache_prefix
 */
#include "redismodule.h"
#include <libpq-fe.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <jansson.h>

#define MODULE_NAME    "pgcache"
#define MODULE_VERSION 1

/* ── Global state ────────────────────────────────────────────────────────── */
typedef struct PGCtx {
    PGconn        *pg_conn;
    char          *pg_host;
    int            pg_port;
    char          *pg_database;
    char          *pg_user;
    char          *pg_password;
    int            default_ttl;
    char          *cache_prefix;
    pthread_mutex_t conn_mutex;
} PGCtx;

static PGCtx *global_ctx = NULL;

/* ── PostgreSQL connection management ───────────────────────────────────── */
static PGconn *get_pg_connection(PGCtx *ctx) {
    pthread_mutex_lock(&ctx->conn_mutex);

    if (ctx->pg_conn == NULL || PQstatus(ctx->pg_conn) != CONNECTION_OK) {
        if (ctx->pg_conn) {
            PQfinish(ctx->pg_conn);
            ctx->pg_conn = NULL;
        }

        char conninfo[512];
        snprintf(conninfo, sizeof(conninfo),
                 "host=%s port=%d dbname=%s user=%s password=%s connect_timeout=5",
                 ctx->pg_host, ctx->pg_port, ctx->pg_database,
                 ctx->pg_user, ctx->pg_password);

        ctx->pg_conn = PQconnectdb(conninfo);

        if (PQstatus(ctx->pg_conn) != CONNECTION_OK) {
            PQfinish(ctx->pg_conn);
            ctx->pg_conn = NULL;
        }
    }

    pthread_mutex_unlock(&ctx->conn_mutex);
    return ctx->pg_conn;
}

/* ── Helpers ─────────────────────────────────────────────────────────────── */
static char *build_cache_key(PGCtx *ctx, const char *table, const char *pk_json) {
    size_t n = strlen(ctx->cache_prefix) + strlen(table) + strlen(pk_json) + 3;
    char *key = RedisModule_Alloc(n);
    snprintf(key, n, "%s%s:%s", ctx->cache_prefix, table, pk_json);
    return key;
}

static void publish_event(RedisModuleCtx *ctx, const char *event_type,
                          const char *table, const char *data) {
    json_t *ev = json_object();
    json_object_set_new(ev, "type",      json_string(event_type));
    json_object_set_new(ev, "table",     json_string(table));
    json_object_set_new(ev, "timestamp", json_integer((json_int_t)time(NULL)));
    if (data)
        json_object_set_new(ev, "data", json_string(data));

    char *s = json_dumps(ev, JSON_COMPACT);
    RedisModule_Call(ctx, "PUBLISH", "cc", "pg_cache_events", s);
    free(s);
    json_decref(ev);
}

static json_t *execute_pg_query(RedisModuleCtx *ctx, PGCtx *pgctx,
                                 const char *query, int *row_count) {
    PGconn *conn = get_pg_connection(pgctx);
    if (!conn) {
        RedisModule_Log(ctx, "warning", "pgcache: no PostgreSQL connection");
        return NULL;
    }

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        RedisModule_Log(ctx, "warning", "pgcache: query failed: %s",
                        PQerrorMessage(conn));
        PQclear(res);
        return NULL;
    }

    int ntuples = PQntuples(res);
    int nfields = PQnfields(res);
    *row_count  = ntuples;

    json_t *arr = json_array();
    for (int i = 0; i < ntuples; i++) {
        json_t *row = json_object();
        for (int j = 0; j < nfields; j++) {
            const char *col = PQfname(res, j);
            if (PQgetisnull(res, i, j))
                json_object_set_new(row, col, json_null());
            else
                json_object_set_new(row, col, json_string(PQgetvalue(res, i, j)));
        }
        json_array_append_new(arr, row);
    }
    PQclear(res);
    return arr;
}

/* Build a WHERE clause string from a JSON object {"col": "val", ...}.
 * Returns a malloc'd string; caller must free(). Returns NULL on error. */
static char *where_clause_from_json(json_t *pk_obj) {
    if (!json_is_object(pk_obj)) return NULL;

    char buf[4096] = "";
    int first = 1;
    const char *key;
    json_t *val;

    json_object_foreach(pk_obj, key, val) {
        if (!first)
            strncat(buf, " AND ", sizeof(buf) - strlen(buf) - 1);

        char part[512];
        if (json_is_string(val)) {
            /* Minimal quoting: double any embedded single-quotes */
            const char *sv = json_string_value(val);
            char safe[256] = "";
            size_t si = 0;
            for (size_t k = 0; k < strlen(sv) && si < sizeof(safe) - 2; k++) {
                if (sv[k] == '\'') safe[si++] = '\'';
                safe[si++] = sv[k];
            }
            safe[si] = '\0';
            snprintf(part, sizeof(part), "%s = '%s'", key, safe);
        } else {
            char *raw = json_dumps(val, JSON_COMPACT | JSON_ENCODE_ANY);
            snprintf(part, sizeof(part), "%s = %s", key, raw ? raw : "NULL");
            free(raw);
        }
        strncat(buf, part, sizeof(buf) - strlen(buf) - 1);
        first = 0;
    }

    return strdup(buf);
}

/* ── PGCACHE.READ table pk_json [ttl] ───────────────────────────────────── */
static int PGCRead_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);
    if (!global_ctx)
        return RedisModule_ReplyWithError(ctx, "ERR pgcache not initialized");

    PGCtx *pgctx        = global_ctx;
    size_t tlen, pklen;
    const char *table   = RedisModule_StringPtrLen(argv[1], &tlen);
    const char *pk_json = RedisModule_StringPtrLen(argv[2], &pklen);

    long long ttl = pgctx->default_ttl;
    if (argc == 4) {
        if (RedisModule_StringToLongLong(argv[3], &ttl) != REDISMODULE_OK || ttl < 1)
            return RedisModule_ReplyWithError(ctx, "ERR invalid ttl");
    }

    char *cache_key = build_cache_key(pgctx, table, pk_json);

    /* Cache hit? */
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "GET", "c", cache_key);
    if (rep && RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_STRING) {
        size_t len;
        const char *data = RedisModule_CallReplyStringPtr(rep, &len);
        publish_event(ctx, "cache_hit", table, NULL);
        RedisModule_ReplyWithStringBuffer(ctx, data, len);
        RedisModule_FreeCallReply(rep);
        RedisModule_Free(cache_key);
        return REDISMODULE_OK;
    }
    if (rep) RedisModule_FreeCallReply(rep);

    /* Cache miss — query Postgres */
    json_t *pk_obj = json_loads(pk_json, 0, NULL);
    if (!pk_obj || !json_is_object(pk_obj)) {
        if (pk_obj) json_decref(pk_obj);
        RedisModule_Free(cache_key);
        return RedisModule_ReplyWithError(ctx, "ERR invalid primary key JSON");
    }

    char *where = where_clause_from_json(pk_obj);
    json_decref(pk_obj);
    if (!where) {
        RedisModule_Free(cache_key);
        return RedisModule_ReplyWithError(ctx, "ERR failed to build WHERE clause");
    }

    char query[4096];
    snprintf(query, sizeof(query), "SELECT * FROM %s WHERE %s LIMIT 1", table, where);
    free(where);

    int row_count = 0;
    json_t *rows = execute_pg_query(ctx, pgctx, query, &row_count);

    if (!rows || json_array_size(rows) == 0) {
        publish_event(ctx, "cache_miss", table, NULL);
        if (rows) json_decref(rows);
        RedisModule_Free(cache_key);
        return RedisModule_ReplyWithNull(ctx);
    }

    char *data_str = json_dumps(json_array_get(rows, 0), JSON_COMPACT);
    json_decref(rows);

    /* Store in cache and reply */
    char ttl_str[32];
    snprintf(ttl_str, sizeof(ttl_str), "%lld", ttl);
    RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl_str, data_str);

    publish_event(ctx, "cache_miss", table, data_str);
    RedisModule_ReplyWithCString(ctx, data_str);

    free(data_str);
    RedisModule_Free(cache_key);
    return REDISMODULE_OK;
}

/* ── PGCACHE.WRITE table pk_json data_json [ttl] ────────────────────────── */
static int PGCWrite_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4 || argc > 5) return RedisModule_WrongArity(ctx);
    if (!global_ctx)
        return RedisModule_ReplyWithError(ctx, "ERR pgcache not initialized");

    PGCtx *pgctx        = global_ctx;
    const char *table   = RedisModule_StringPtrLen(argv[1], NULL);
    const char *pk_json = RedisModule_StringPtrLen(argv[2], NULL);
    const char *data    = RedisModule_StringPtrLen(argv[3], NULL);

    long long ttl = pgctx->default_ttl;
    if (argc == 5) {
        if (RedisModule_StringToLongLong(argv[4], &ttl) != REDISMODULE_OK || ttl < 1)
            return RedisModule_ReplyWithError(ctx, "ERR invalid ttl");
    }

    char *cache_key = build_cache_key(pgctx, table, pk_json);

    char ttl_str[32];
    snprintf(ttl_str, sizeof(ttl_str), "%lld", ttl);
    RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl_str, data);

    publish_event(ctx, "cache_write", table, data);
    RedisModule_Free(cache_key);

    return RedisModule_ReplyWithCString(ctx, "OK");
}

/* ── PGCACHE.INVALIDATE table pk_json ───────────────────────────────────── */
static int PGCInvalidate_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    if (!global_ctx)
        return RedisModule_ReplyWithError(ctx, "ERR pgcache not initialized");

    PGCtx *pgctx        = global_ctx;
    const char *table   = RedisModule_StringPtrLen(argv[1], NULL);
    const char *pk_json = RedisModule_StringPtrLen(argv[2], NULL);

    char *cache_key = build_cache_key(pgctx, table, pk_json);
    RedisModule_Call(ctx, "DEL", "c", cache_key);
    publish_event(ctx, "cache_invalidate", table, pk_json);
    RedisModule_Free(cache_key);

    return RedisModule_ReplyWithCString(ctx, "OK");
}

/* ── PGCACHE.MULTIREAD table pk_json_array [ttl] ────────────────────────── */
static int PGCMultiRead_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);
    if (!global_ctx)
        return RedisModule_ReplyWithError(ctx, "ERR pgcache not initialized");

    PGCtx *pgctx          = global_ctx;
    const char *table     = RedisModule_StringPtrLen(argv[1], NULL);
    const char *pks_json  = RedisModule_StringPtrLen(argv[2], NULL);

    long long ttl = pgctx->default_ttl;
    if (argc == 4) {
        if (RedisModule_StringToLongLong(argv[3], &ttl) != REDISMODULE_OK || ttl < 1)
            return RedisModule_ReplyWithError(ctx, "ERR invalid ttl");
    }

    json_t *pk_array = json_loads(pks_json, 0, NULL);
    if (!pk_array || !json_is_array(pk_array)) {
        if (pk_array) json_decref(pk_array);
        return RedisModule_ReplyWithError(ctx, "ERR invalid primary keys JSON array");
    }

    int pk_count = (int)json_array_size(pk_array);

    /* Per-entry state */
    char  **cache_keys    = RedisModule_Calloc(pk_count, sizeof(char *));
    char  **cached_values = RedisModule_Calloc(pk_count, sizeof(char *));
    int    *cache_hits    = RedisModule_Calloc(pk_count, sizeof(int));

    char ttl_str[32];
    snprintf(ttl_str, sizeof(ttl_str), "%lld", ttl);

    /* Pass 1: check cache for each key */
    for (int i = 0; i < pk_count; i++) {
        json_t *pk_obj  = json_array_get(pk_array, i);
        char   *pk_str  = json_dumps(pk_obj, JSON_COMPACT);
        cache_keys[i]   = build_cache_key(pgctx, table, pk_str);
        free(pk_str);

        RedisModuleCallReply *rep = RedisModule_Call(ctx, "GET", "c", cache_keys[i]);
        if (rep && RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_STRING) {
            size_t len;
            const char *v = RedisModule_CallReplyStringPtr(rep, &len);
            char *copy = RedisModule_Alloc(len + 1);
            memcpy(copy, v, len);
            copy[len] = '\0';
            cached_values[i] = copy;
            cache_hits[i]    = 1;
        }
        if (rep) RedisModule_FreeCallReply(rep);
    }

    /* Pass 2: batch-fetch misses from PostgreSQL */
    char or_conditions[16384] = "";
    int  miss_count = 0;

    for (int i = 0; i < pk_count; i++) {
        if (cache_hits[i]) continue;

        json_t *pk_obj = json_array_get(pk_array, i);
        char *where    = where_clause_from_json(pk_obj);
        if (!where) continue;

        if (miss_count > 0)
            strncat(or_conditions, " OR ", sizeof(or_conditions) - strlen(or_conditions) - 1);

        char cond[2048];
        snprintf(cond, sizeof(cond), "(%s)", where);
        strncat(or_conditions, cond, sizeof(or_conditions) - strlen(or_conditions) - 1);
        free(where);
        miss_count++;
    }

    json_t *pg_rows = NULL;
    if (miss_count > 0) {
        char full_query[20480];
        snprintf(full_query, sizeof(full_query),
                 "SELECT * FROM %s WHERE %s", table, or_conditions);
        int row_count = 0;
        pg_rows = execute_pg_query(ctx, pgctx, full_query, &row_count);

        /* Cache each row under the key built from the row's own PK fields.
         * We re-serialize each row as JSON and use it as the cached value. */
        if (pg_rows) {
            for (size_t r = 0; r < json_array_size(pg_rows); r++) {
                json_t *row = json_array_get(pg_rows, r);
                char *row_json = json_dumps(row, JSON_COMPACT);

                /* Match this row back to the miss entries by checking WHERE
                 * equality for each miss. Simple O(misses * rows) — fine for
                 * typical batch sizes. */
                for (int i = 0; i < pk_count; i++) {
                    if (cache_hits[i]) continue;
                    json_t *pk_obj = json_array_get(pk_array, i);
                    int match = 1;
                    const char *k;
                    json_t *pv;
                    json_object_foreach(pk_obj, k, pv) {
                        json_t *rv = json_object_get(row, k);
                        if (!rv) { match = 0; break; }
                        if (json_is_string(pv) && json_is_string(rv)) {
                            if (strcmp(json_string_value(pv), json_string_value(rv)) != 0) {
                                match = 0; break;
                            }
                        }
                    }
                    if (match && !cached_values[i]) {
                        size_t rjlen = strlen(row_json);
                        char *copy = RedisModule_Alloc(rjlen + 1);
                        memcpy(copy, row_json, rjlen + 1);
                        cached_values[i] = copy;
                        cache_hits[i]    = 1;
                        RedisModule_Call(ctx, "SETEX", "ccc", cache_keys[i], ttl_str, row_json);
                    }
                }
                free(row_json);
            }
        }
    }

    /* Build flat array reply: [json_or_null, ...] */
    RedisModule_ReplyWithArray(ctx, pk_count);
    for (int i = 0; i < pk_count; i++) {
        if (cached_values[i])
            RedisModule_ReplyWithCString(ctx, cached_values[i]);
        else
            RedisModule_ReplyWithNull(ctx);
    }

    /* Cleanup */
    for (int i = 0; i < pk_count; i++) {
        RedisModule_Free(cache_keys[i]);
        if (cached_values[i]) RedisModule_Free(cached_values[i]);
    }
    RedisModule_Free(cache_keys);
    RedisModule_Free(cached_values);
    RedisModule_Free(cache_hits);
    if (pg_rows) json_decref(pg_rows);
    json_decref(pk_array);

    return REDISMODULE_OK;
}

/* ── Module lifecycle ────────────────────────────────────────────────────── */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, MODULE_NAME, MODULE_VERSION, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    global_ctx = RedisModule_Calloc(1, sizeof(PGCtx));
    if (!global_ctx) return REDISMODULE_ERR;

    global_ctx->default_ttl  = 3600;
    global_ctx->cache_prefix = RedisModule_Strdup("pg_cache:");
    global_ctx->pg_port      = 5432;
    pthread_mutex_init(&global_ctx->conn_mutex, NULL);

    for (int i = 0; i + 1 < argc; i += 2) {
        const char *param = RedisModule_StringPtrLen(argv[i],     NULL);
        const char *value = RedisModule_StringPtrLen(argv[i + 1], NULL);

        if      (strcmp(param, "pg_host")     == 0) global_ctx->pg_host     = RedisModule_Strdup(value);
        else if (strcmp(param, "pg_port")     == 0) global_ctx->pg_port     = atoi(value);
        else if (strcmp(param, "pg_database") == 0) global_ctx->pg_database = RedisModule_Strdup(value);
        else if (strcmp(param, "pg_user")     == 0) global_ctx->pg_user     = RedisModule_Strdup(value);
        else if (strcmp(param, "pg_password") == 0) global_ctx->pg_password = RedisModule_Strdup(value);
        else if (strcmp(param, "default_ttl") == 0) global_ctx->default_ttl = atoi(value);
        else if (strcmp(param, "cache_prefix") == 0) {
            RedisModule_Free(global_ctx->cache_prefix);
            global_ctx->cache_prefix = RedisModule_Strdup(value);
        }
    }

    if (!global_ctx->pg_host)     global_ctx->pg_host     = RedisModule_Strdup("localhost");
    if (!global_ctx->pg_database) global_ctx->pg_database = RedisModule_Strdup("postgres");
    if (!global_ctx->pg_user)     global_ctx->pg_user     = RedisModule_Strdup("postgres");
    if (!global_ctx->pg_password) global_ctx->pg_password = RedisModule_Strdup("");

    if (RedisModule_CreateCommand(ctx, "pgcache.read",      PGCRead_Command,      "readonly",        0, 0, 0) == REDISMODULE_ERR) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "pgcache.write",     PGCWrite_Command,     "write",           0, 0, 0) == REDISMODULE_ERR) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "pgcache.invalidate",PGCInvalidate_Command,"write",           0, 0, 0) == REDISMODULE_ERR) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "pgcache.multiread", PGCMultiRead_Command, "readonly",        0, 0, 0) == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModule_Log(ctx, "notice", "pgcache module loaded (PostgreSQL read/write-through cache)");
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    (void)ctx;
    if (global_ctx) {
        pthread_mutex_lock(&global_ctx->conn_mutex);
        if (global_ctx->pg_conn) {
            PQfinish(global_ctx->pg_conn);
            global_ctx->pg_conn = NULL;
        }
        pthread_mutex_unlock(&global_ctx->conn_mutex);
        pthread_mutex_destroy(&global_ctx->conn_mutex);

        RedisModule_Free(global_ctx->pg_host);
        RedisModule_Free(global_ctx->pg_database);
        RedisModule_Free(global_ctx->pg_user);
        RedisModule_Free(global_ctx->pg_password);
        RedisModule_Free(global_ctx->cache_prefix);
        RedisModule_Free(global_ctx);
        global_ctx = NULL;
    }
    return REDISMODULE_OK;
}
