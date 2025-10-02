#include <redismodule.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <jansson.h>
#include <unistd.h>
#include <sys/select.h>

/* Module metadata */
#define MODULE_NAME "pgcache"
#define MODULE_VERSION 1

/* Global state */
typedef struct PGCtx {
    PGconn *pg_conn;
    char *pg_host;
    int pg_port;
    char *pg_database;
    char *pg_user;
    char *pg_password;
    int default_ttl;
    char *cache_prefix;
    RedisModuleCtx *redis_ctx;
    pthread_mutex_t conn_mutex;

    // Notification handling
    int enable_notifications;
    PGconn *notify_conn;
    pthread_t notify_thread;
    int notify_thread_running;

    // WAL reading
    int enable_wal;
    char *wal_slot_name;
    char *wal_publication_name;
    PGconn *wal_conn;
    pthread_t wal_thread;
    int wal_thread_running;

    // Watch forwarding
    int enable_watch_forwarding;
} PGCtx;

static PGCtx *global_ctx = NULL;

/* Helper functions */
static PGconn* get_pg_connection(PGCtx *ctx) {
    pthread_mutex_lock(&ctx->conn_mutex);

    if (ctx->pg_conn == NULL || PQstatus(ctx->pg_conn) != CONNECTION_OK) {
        if (ctx->pg_conn) {
            PQfinish(ctx->pg_conn);
        }

        char conninfo[512];
        snprintf(conninfo, sizeof(conninfo),
                "host=%s port=%d dbname=%s user=%s password=%s",
                ctx->pg_host, ctx->pg_port, ctx->pg_database,
                ctx->pg_user, ctx->pg_password);

        ctx->pg_conn = PQconnectdb(conninfo);

        if (PQstatus(ctx->pg_conn) != CONNECTION_OK) {
            RedisModule_Log(ctx->redis_ctx, "warning",
                           "Failed to connect to PostgreSQL: %s",
                           PQerrorMessage(ctx->pg_conn));
            PQfinish(ctx->pg_conn);
            ctx->pg_conn = NULL;
        }
    }

    pthread_mutex_unlock(&ctx->conn_mutex);
    return ctx->pg_conn;
}

static char* build_cache_key(PGCtx *ctx, const char *table, const char *primary_key_json) {
    size_t key_len = strlen(ctx->cache_prefix) + strlen(table) + strlen(primary_key_json) + 3;
    char *cache_key = RedisModule_Alloc(key_len);
    snprintf(cache_key, key_len, "%s%s:%s", ctx->cache_prefix, table, primary_key_json);
    return cache_key;
}

static void publish_event(PGCtx *ctx, const char *event_type, const char *table, const char *data) {
    RedisModuleCtx *rctx = ctx->redis_ctx;

    // Create event JSON
    json_t *event = json_object();
    json_object_set_new(event, "type", json_string(event_type));
    json_object_set_new(event, "table", json_string(table));
    json_object_set_new(event, "timestamp", json_real((double)time(NULL)));

    if (data) {
        json_object_set_new(event, "data", json_string(data));
    }

    char *event_str = json_dumps(event, JSON_COMPACT);

    // Publish to Redis pubsub
    RedisModule_Call(rctx, "PUBLISH", "cc", "pg_cache_events", event_str);

    free(event_str);
    json_decref(event);
}

static json_t* execute_pg_query(PGCtx *ctx, const char *query, int *row_count) {
    PGconn *conn = get_pg_connection(ctx);
    if (!conn) {
        return NULL;
    }

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        RedisModule_Log(ctx->redis_ctx, "warning",
                       "PostgreSQL query failed: %s", PQerrorMessage(conn));
        PQclear(res);
        return NULL;
    }

    int ntuples = PQntuples(res);
    int nfields = PQnfields(res);
    *row_count = ntuples;

    json_t *result_array = json_array();

    for (int i = 0; i < ntuples; i++) {
        json_t *row_obj = json_object();

        for (int j = 0; j < nfields; j++) {
            const char *colname = PQfname(res, j);
            const char *value = PQgetvalue(res, i, j);

            if (PQgetisnull(res, i, j)) {
                json_object_set_new(row_obj, colname, json_null());
            } else {
                json_object_set_new(row_obj, colname, json_string(value));
            }
        }

        json_array_append_new(result_array, row_obj);
    }

    PQclear(res);
    return result_array;
}

/* Notification handling functions */
static void setup_pg_notifications(PGCtx *ctx) {
    if (!ctx->enable_notifications) return;

    ctx->notify_conn = PQconnectdb(""); // Use same connection string as main conn
    if (PQstatus(ctx->notify_conn) != CONNECTION_OK) {
        RedisModule_Log(ctx->redis_ctx, "warning",
                       "Failed to connect for notifications: %s",
                       PQerrorMessage(ctx->notify_conn));
        PQfinish(ctx->notify_conn);
        ctx->notify_conn = NULL;
        return;
    }

    // Listen for cache invalidation events
    PGresult *res = PQexec(ctx->notify_conn, "LISTEN cache_invalidation");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        RedisModule_Log(ctx->redis_ctx, "warning",
                       "Failed to set up LISTEN: %s", PQerrorMessage(ctx->notify_conn));
        PQclear(res);
        PQfinish(ctx->notify_conn);
        ctx->notify_conn = NULL;
        return;
    }
    PQclear(res);

    RedisModule_Log(ctx->redis_ctx, "notice", "PostgreSQL notifications enabled");
}

static void* notification_listener_thread(void *arg) {
    PGCtx *ctx = (PGCtx*)arg;
    ctx->notify_thread_running = 1;

    RedisModule_Log(ctx->redis_ctx, "notice", "Notification listener thread started");

    while (ctx->notify_thread_running && ctx->notify_conn) {
        // Wait for notifications with timeout
        if (PQconsumeInput(ctx->notify_conn) == 0) {
            RedisModule_Log(ctx->redis_ctx, "warning", "Lost connection to PostgreSQL");
            break;
        }

        PGnotify *notify;
        while ((notify = PQnotifies(ctx->notify_conn)) != NULL) {
            // Parse notification payload
            json_t *payload = json_loads(notify->extra, 0, NULL);
            if (payload && json_is_object(payload)) {
                json_t *table_json = json_object_get(payload, "table");
                json_t *operation_json = json_object_get(payload, "operation");
                json_t *old_data = json_object_get(payload, "old_data");
                json_t *new_data = json_object_get(payload, "new_data");

                if (table_json && json_is_string(table_json)) {
                    const char *table = json_string_value(table_json);
                    const char *operation = operation_json ? json_string_value(operation_json) : "unknown";

                    // Build primary key from old_data or new_data
                    json_t *pk_data = old_data ? old_data : new_data;
                    if (pk_data && json_is_object(pk_data)) {
                        char *pk_json = json_dumps(pk_data, JSON_COMPACT);

                        // Invalidate cache entry
                        char *cache_key = build_cache_key(ctx, table, pk_json);

                        RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
                        RedisModule_Call(rctx, "DEL", "c", cache_key);
                        RedisModule_FreeThreadSafeContext(rctx);

                        // Publish invalidation event
                        char event_data[1024];
                        snprintf(event_data, sizeof(event_data),
                                "{\"operation\":\"%s\",\"table\":\"%s\",\"key\":%s}",
                                operation, table, pk_json);

                        publish_event(ctx, "pg_invalidation", table, event_data);

                        RedisModule_Log(ctx->redis_ctx, "notice",
                                       "Invalidated cache for table %s, key %s", table, pk_json);

                        free(pk_json);
                        RedisModule_Free(cache_key);
                    }
                }
            }

            if (payload) json_decref(payload);
            PQfreemem(notify);
        }

        // Sleep briefly to avoid busy waiting
        usleep(100000); // 100ms
    }

    ctx->notify_thread_running = 0;
    RedisModule_Log(ctx->redis_ctx, "notice", "Notification listener thread stopped");
    return NULL;
}

/* WAL reading functions */
static void setup_wal_reading(PGCtx *ctx) {
    if (!ctx->enable_wal) return;

    // Create logical replication slot if it doesn't exist
    char create_slot_query[512];
    snprintf(create_slot_query, sizeof(create_slot_query),
             "SELECT * FROM pg_create_logical_replication_slot('%s', 'pgoutput')",
             ctx->wal_slot_name);

    PGconn *temp_conn = get_pg_connection(ctx);
    if (temp_conn) {
        PGresult *res = PQexec(temp_conn, create_slot_query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            RedisModule_Log(ctx->redis_ctx, "warning",
                           "Failed to create replication slot: %s", PQerrorMessage(temp_conn));
        }
        PQclear(res);
    }

    // Connect for WAL streaming
    ctx->wal_conn = PQconnectdb(""); // Use same connection string
    if (PQstatus(ctx->wal_conn) != CONNECTION_OK) {
        RedisModule_Log(ctx->redis_ctx, "warning",
                       "Failed to connect for WAL reading: %s",
                       PQerrorMessage(ctx->wal_conn));
        PQfinish(ctx->wal_conn);
        ctx->wal_conn = NULL;
        return;
    }

    // Start logical replication
    char replication_query[1024];
    snprintf(replication_query, sizeof(replication_query),
             "START_REPLICATION SLOT %s LOGICAL 0/0 (proto_version '1', publication_names '%s')",
             ctx->wal_slot_name, ctx->wal_publication_name);

    if (PQsendQuery(ctx->wal_conn, replication_query) == 0) {
        RedisModule_Log(ctx->redis_ctx, "warning",
                       "Failed to start WAL replication: %s", PQerrorMessage(ctx->wal_conn));
        PQfinish(ctx->wal_conn);
        ctx->wal_conn = NULL;
        return;
    }

    RedisModule_Log(ctx->redis_ctx, "notice", "WAL reading enabled");
}

static void* wal_reader_thread(void *arg) {
    PGCtx *ctx = (PGCtx*)arg;
    ctx->wal_thread_running = 1;

    RedisModule_Log(ctx->redis_ctx, "notice", "WAL reader thread started");

    while (ctx->wal_thread_running && ctx->wal_conn) {
        // Read WAL data
        char *buffer = NULL;
        int len = PQgetCopyData(ctx->wal_conn, &buffer, 0);

        if (len > 0) {
            // Process WAL message
            // This is a simplified implementation - real WAL parsing would be more complex
            RedisModule_Log(ctx->redis_ctx, "notice", "Received WAL data: %d bytes", len);

            // Publish WAL event
            publish_event(ctx, "wal_change", "system", buffer);

            PQfreemem(buffer);

        } else if (len == -1) {
            // Error or connection closed
            RedisModule_Log(ctx->redis_ctx, "warning", "WAL connection error");
            break;
        } else if (len == -2) {
            // No data available
            usleep(100000); // 100ms
        }
    }

    ctx->wal_thread_running = 0;
    RedisModule_Log(ctx->redis_ctx, "notice", "WAL reader thread stopped");
    return NULL;
}

/* Watch forwarding functions */
static void setup_watch_forwarding(PGCtx *ctx) {
    if (!ctx->enable_watch_forwarding) return;

    RedisModule_Log(ctx->redis_ctx, "notice", "Watch forwarding enabled");
}

/* Redis commands */

/* PGCACHE.READ <table> <primary_key_json> [TTL] */
int PGCRead_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_key_json = RedisModule_StringPtrLen(argv[2], NULL);
    int ttl = (argc == 4) ? atoi(RedisModule_StringPtrLen(argv[3], NULL)) : pgctx->default_ttl;

    // Build cache key
    char *cache_key = build_cache_key(pgctx, table, primary_key_json);

    // Try to get from cache first
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "c", cache_key);

    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING) {
        // Cache hit
        size_t len;
        const char *cached_data = RedisModule_CallReplyStringPtr(reply, &len);

        publish_event(pgctx, "cache_hit", table, cached_data);

        // Update access time for LRU (optional)
        RedisModule_Call(ctx, "HINCRBY", "ccc", cache_key, "hits", "1");

        RedisModule_ReplyWithStringBuffer(ctx, cached_data, len);
        RedisModule_FreeCallReply(reply);
        RedisModule_Free(cache_key);
        return REDISMODULE_OK;
    }

    RedisModule_FreeCallReply(reply);

    // Cache miss - query PostgreSQL
    // Build query from primary key JSON
    json_t *pk_data = json_loads(primary_key_json, 0, NULL);
    if (!pk_data || !json_is_object(pk_data)) {
        RedisModule_ReplyWithError(ctx, "Invalid primary key JSON");
        if (pk_data) json_decref(pk_data);
        RedisModule_Free(cache_key);
        return REDISMODULE_OK;
    }

    // Build WHERE clause
    const char *key;
    json_t *value;
    char where_clause[2048] = "";
    int first = 1;

    json_object_foreach(pk_data, key, value) {
        if (!first) {
            strncat(where_clause, " AND ", sizeof(where_clause) - strlen(where_clause) - 1);
        }
        char condition[256];
        if (json_is_string(value)) {
            snprintf(condition, sizeof(condition), "%s = '%s'",
                    key, json_string_value(value));
        } else {
            snprintf(condition, sizeof(condition), "%s = %s",
                    key, json_string_value(value));
        }
        strncat(where_clause, condition, sizeof(where_clause) - strlen(where_clause) - 1);
        first = 0;
    }

    char query[4096];
    snprintf(query, sizeof(query), "SELECT * FROM %s WHERE %s", table, where_clause);

    json_decref(pk_data);

    int row_count = 0;
    json_t *pg_result = execute_pg_query(pgctx, query, &row_count);

    if (!pg_result || json_array_size(pg_result) == 0) {
        // No data found
        publish_event(pgctx, "cache_miss", table, NULL);
        RedisModule_ReplyWithNull(ctx);
        if (pg_result) json_decref(pg_result);
        RedisModule_Free(cache_key);
        return REDISMODULE_OK;
    }

    // Get first row and cache it
    json_t *row_data = json_array_get(pg_result, 0);
    char *data_str = json_dumps(row_data, JSON_COMPACT);

    // Store in Redis cache
    RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl, data_str);

    // Update miss statistics
    RedisModule_Call(ctx, "HINCRBY", "ccc", cache_key, "misses", "1");

    publish_event(pgctx, "cache_miss", table, data_str);

    RedisModule_ReplyWithStringBuffer(ctx, data_str, strlen(data_str));

    free(data_str);
    json_decref(pg_result);
    RedisModule_Free(cache_key);

    return REDISMODULE_OK;
}

/* PGCACHE.WRITE <table> <primary_key_json> <data_json> [TTL] */
int PGCWrite_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4 || argc > 5) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_key_json = RedisModule_StringPtrLen(argv[2], NULL);
    const char *data_json = RedisModule_StringPtrLen(argv[3], NULL);
    int ttl = (argc == 5) ? atoi(RedisModule_StringPtrLen(argv[4], NULL)) : pgctx->default_ttl;

    // Build cache key
    char *cache_key = build_cache_key(pgctx, table, primary_key_json);

    // Store in Redis cache
    RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl, data_json);

    publish_event(pgctx, "cache_write", table, data_json);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_Free(cache_key);

    return REDISMODULE_OK;
}

/* PGCACHE.INVALIDATE <table> <primary_key_json> */
int PGCInvalidate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_key_json = RedisModule_StringPtrLen(argv[2], NULL);

    // Build cache key
    char *cache_key = build_cache_key(pgctx, table, primary_key_json);

    // Delete from cache
    RedisModule_Call(ctx, "DEL", "c", cache_key);

    publish_event(pgctx, "cache_invalidate", table, primary_key_json);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_Free(cache_key);

    return REDISMODULE_OK;
}

/* PGCACHE.MULTIREAD <table> <primary_keys_json_array> [TTL] */
int PGCMultiRead_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_keys_json = RedisModule_StringPtrLen(argv[2], NULL);
    int ttl = (argc == 4) ? atoi(RedisModule_StringPtrLen(argv[3], NULL)) : pgctx->default_ttl;

    // Parse primary keys array
    json_t *pk_array = json_loads(primary_keys_json, 0, NULL);
    if (!pk_array || !json_is_array(pk_array)) {
        RedisModule_ReplyWithError(ctx, "Invalid primary keys JSON array");
        if (pk_array) json_decref(pk_array);
        return REDISMODULE_OK;
    }

    int pk_count = json_array_size(pk_array);

    // Check cache for all keys first
    char **cache_keys = RedisModule_Alloc(sizeof(char*) * pk_count);
    int *cache_hits = RedisModule_Alloc(sizeof(int) * pk_count);
    const char **cached_values = RedisModule_Alloc(sizeof(char*) * pk_count);

    for (int i = 0; i < pk_count; i++) {
        json_t *pk_obj = json_array_get(pk_array, i);
        char *pk_json = json_dumps(pk_obj, JSON_COMPACT);
        cache_keys[i] = build_cache_key(pgctx, table, pk_json);

        RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "c", cache_keys[i]);
        if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING) {
            size_t len;
            cached_values[i] = RedisModule_CallReplyStringPtr(reply, &len);
            cache_hits[i] = 1;

            // Copy the string since the reply will be freed
            char *value_copy = RedisModule_Alloc(len + 1);
            memcpy(value_copy, cached_values[i], len);
            value_copy[len] = '\0';
            cached_values[i] = value_copy;
        } else {
            cache_hits[i] = 0;
            cached_values[i] = NULL;
        }
        RedisModule_FreeCallReply(reply);
        free(pk_json);
    }

    // Build query for cache misses
    char query[8192] = "";
    int miss_count = 0;

    for (int i = 0; i < pk_count; i++) {
        if (!cache_hits[i]) {
            json_t *pk_obj = json_array_get(pk_array, i);
            char *pk_json = json_dumps(pk_obj, JSON_COMPACT);

            if (miss_count > 0) {
                strncat(query, " OR ", sizeof(query) - strlen(query) - 1);
            }

            // Build OR condition for this primary key
            const char *key;
            json_t *value;
            char condition[1024] = "(";
            int first = 1;

            json_object_foreach(pk_obj, key, value) {
                if (!first) {
                    strncat(condition, " AND ", sizeof(condition) - strlen(condition) - 1);
                }
                char part[256];
                if (json_is_string(value)) {
                    snprintf(part, sizeof(part), "%s = '%s'", key, json_string_value(value));
                } else {
                    snprintf(part, sizeof(part), "%s = %s", key, json_string_value(value));
                }
                strncat(condition, part, sizeof(condition) - strlen(condition) - 1);
                first = 0;
            }
            strncat(condition, ")", sizeof(condition) - strlen(condition) - 1);

            strncat(query, condition, sizeof(query) - strlen(query) - 1);
            miss_count++;
            free(pk_json);
        }
    }

    if (miss_count > 0) {
        char full_query[16384];
        snprintf(full_query, sizeof(full_query), "SELECT * FROM %s WHERE %s", table, query);

        int row_count = 0;
        json_t *pg_result = execute_pg_query(pgctx, full_query, &row_count);

        if (pg_result && json_array_size(pg_result) > 0) {
            // Cache the results
            for (size_t i = 0; i < json_array_size(pg_result); i++) {
                json_t *row = json_array_get(pg_result, i);
                char *row_json = json_dumps(row, JSON_COMPACT);

                // Find corresponding primary key to build cache key
                // This is simplified - in practice you'd need to extract PK from row
                char cache_key[1024];
                snprintf(cache_key, sizeof(cache_key), "%s%s:row_%zu", pgctx->cache_prefix, table, i);

                RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl, row_json);
                free(row_json);
            }
        }

        if (pg_result) json_decref(pg_result);
    }

    // Build response
    json_t *response_array = json_array();

    for (int i = 0; i < pk_count; i++) {
        if (cache_hits[i]) {
            json_t *cached_obj = json_loads(cached_values[i], 0, NULL);
            if (cached_obj) {
                json_array_append_new(response_array, cached_obj);
            }
            RedisModule_Free((char*)cached_values[i]);
        } else {
            // For misses, we return null (could be enhanced to return from DB)
            json_array_append_new(response_array, json_null());
        }
    }

    char *response_str = json_dumps(response_array, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, response_str, strlen(response_str));

    free(response_str);
    json_decref(response_array);
    json_decref(pk_array);

    for (int i = 0; i < pk_count; i++) {
        RedisModule_Free(cache_keys[i]);
    }
    RedisModule_Free(cache_keys);
    RedisModule_Free(cache_hits);
    RedisModule_Free(cached_values);

    return REDISMODULE_OK;
}

/* PGCACHE.SETUP.NOTIFICATIONS */
int PGCSetupNotifications_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *enable_str = RedisModule_StringPtrLen(argv[1], NULL);
    pgctx->enable_notifications = strcmp(enable_str, "1") == 0 ||
                                  strcmp(enable_str, "true") == 0 ||
                                  strcmp(enable_str, "yes") == 0;

    if (pgctx->enable_notifications) {
        setup_pg_notifications(pgctx);
        if (pgctx->notify_conn) {
            // Start notification listener thread
            pthread_create(&pgctx->notify_thread, NULL, notification_listener_thread, pgctx);
        }
    } else {
        // Stop notification thread
        if (pgctx->notify_thread_running) {
            pgctx->notify_thread_running = 0;
            pthread_join(pgctx->notify_thread, NULL);
        }
        if (pgctx->notify_conn) {
            PQfinish(pgctx->notify_conn);
            pgctx->notify_conn = NULL;
        }
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* PGCACHE.SETUP.WAL <slot_name> <publication_name> */
int PGCSetupWAL_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *slot_name = RedisModule_StringPtrLen(argv[1], NULL);
    const char *publication_name = RedisModule_StringPtrLen(argv[2], NULL);

    pgctx->enable_wal = 1;
    pgctx->wal_slot_name = RedisModule_Strdup(slot_name);
    pgctx->wal_publication_name = RedisModule_Strdup(publication_name);

    setup_wal_reading(pgctx);
    if (pgctx->wal_conn) {
        // Start WAL reader thread
        pthread_create(&pgctx->wal_thread, NULL, wal_reader_thread, pgctx);
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* PGCACHE.SETUP.WATCHFORWARDING */
int PGCSetupWatchForwarding_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *enable_str = RedisModule_StringPtrLen(argv[1], NULL);
    pgctx->enable_watch_forwarding = strcmp(enable_str, "1") == 0 ||
                                     strcmp(enable_str, "true") == 0 ||
                                     strcmp(enable_str, "yes") == 0;

    setup_watch_forwarding(pgctx);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* PGCACHE.STATUS */
int PGCStatus_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    json_t *status = json_object();
    json_object_set_new(status, "notifications_enabled", json_boolean(pgctx->enable_notifications));
    json_object_set_new(status, "notifications_running", json_boolean(pgctx->notify_thread_running));
    json_object_set_new(status, "wal_enabled", json_boolean(pgctx->enable_wal));
    json_object_set_new(status, "wal_running", json_boolean(pgctx->wal_thread_running));
    json_object_set_new(status, "watch_forwarding_enabled", json_boolean(pgctx->enable_watch_forwarding));
    json_object_set_new(status, "cache_prefix", json_string(pgctx->cache_prefix));
    json_object_set_new(status, "default_ttl", json_integer(pgctx->default_ttl));

    char *status_str = json_dumps(status, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, status_str, strlen(status_str));

    free(status_str);
    json_decref(status);
    return REDISMODULE_OK;
}

/* PGCACHE.WAL.LIST */
int PGCWALList_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    // Query for replication slots
    const char *query = "SELECT slot_name, plugin, slot_type, database, active FROM pg_replication_slots";
    int row_count = 0;
    json_t *result = execute_pg_query(pgctx, query, &row_count);

    if (!result) {
        return RedisModule_ReplyWithError(ctx, "Failed to query replication slots");
    }

    char *result_str = json_dumps(result, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, result_str, strlen(result_str));

    free(result_str);
    json_decref(result);
    return REDISMODULE_OK;
}

/* PGCACHE.WATCH <key> */
int PGCWatch_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx || !pgctx->enable_watch_forwarding) {
        return RedisModule_ReplyWithError(ctx, "Watch forwarding not enabled");
    }

    const char *key = RedisModule_StringPtrLen(argv[1], NULL);

    // Forward watch to Redis pubsub channel
    char channel[512];
    snprintf(channel, sizeof(channel), "pgcache_watch:%s", key);

    // Publish watch event
    json_t *watch_event = json_object();
    json_object_set_new(watch_event, "type", json_string("watch_started"));
    json_object_set_new(watch_event, "key", json_string(key));
    json_object_set_new(watch_event, "timestamp", json_real((double)time(NULL)));

    char *event_str = json_dumps(watch_event, JSON_COMPACT);
    RedisModule_Call(ctx, "PUBLISH", "cc", channel, event_str);

    free(event_str);
    json_decref(watch_event);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* Module initialization */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, MODULE_NAME, MODULE_VERSION, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Initialize global context
    global_ctx = RedisModule_Alloc(sizeof(PGCtx));
    if (!global_ctx) {
        return REDISMODULE_ERR;
    }

    memset(global_ctx, 0, sizeof(PGCtx));
    global_ctx->redis_ctx = ctx;
    global_ctx->default_ttl = 3600;
    global_ctx->cache_prefix = RedisModule_Strdup("pg_cache:");

    // Initialize new fields
    global_ctx->enable_notifications = 0;
    global_ctx->notify_thread_running = 0;
    global_ctx->enable_wal = 0;
    global_ctx->wal_thread_running = 0;
    global_ctx->enable_watch_forwarding = 0;
    global_ctx->wal_slot_name = RedisModule_Strdup("pgcache_slot");
    global_ctx->wal_publication_name = RedisModule_Strdup("pgcache_pub");

    pthread_mutex_init(&global_ctx->conn_mutex, NULL);

    // Parse module arguments
    for (int i = 0; i < argc; i += 2) {
        const char *param = RedisModule_StringPtrLen(argv[i], NULL);
        const char *value = RedisModule_StringPtrLen(argv[i + 1], NULL);

        if (strcmp(param, "pg_host") == 0) {
            global_ctx->pg_host = RedisModule_Strdup(value);
        } else if (strcmp(param, "pg_port") == 0) {
            global_ctx->pg_port = atoi(value);
        } else if (strcmp(param, "pg_database") == 0) {
            global_ctx->pg_database = RedisModule_Strdup(value);
        } else if (strcmp(param, "pg_user") == 0) {
            global_ctx->pg_user = RedisModule_Strdup(value);
        } else if (strcmp(param, "pg_password") == 0) {
            global_ctx->pg_password = RedisModule_Strdup(value);
        } else if (strcmp(param, "default_ttl") == 0) {
            global_ctx->default_ttl = atoi(value);
        } else if (strcmp(param, "cache_prefix") == 0) {
            RedisModule_Free(global_ctx->cache_prefix);
            global_ctx->cache_prefix = RedisModule_Strdup(value);
        } else if (strcmp(param, "enable_notifications") == 0) {
            global_ctx->enable_notifications = strcmp(value, "1") == 0 ||
                                              strcmp(value, "true") == 0 ||
                                              strcmp(value, "yes") == 0;
        } else if (strcmp(param, "enable_wal") == 0) {
            global_ctx->enable_wal = strcmp(value, "1") == 0 ||
                                     strcmp(value, "true") == 0 ||
                                     strcmp(value, "yes") == 0;
        } else if (strcmp(param, "wal_slot_name") == 0) {
            RedisModule_Free(global_ctx->wal_slot_name);
            global_ctx->wal_slot_name = RedisModule_Strdup(value);
        } else if (strcmp(param, "wal_publication_name") == 0) {
            RedisModule_Free(global_ctx->wal_publication_name);
            global_ctx->wal_publication_name = RedisModule_Strdup(value);
        } else if (strcmp(param, "enable_watch_forwarding") == 0) {
            global_ctx->enable_watch_forwarding = strcmp(value, "1") == 0 ||
                                                 strcmp(value, "true") == 0 ||
                                                 strcmp(value, "yes") == 0;
        }
    }

    // Set defaults if not provided
    if (!global_ctx->pg_host) global_ctx->pg_host = RedisModule_Strdup("localhost");
    if (!global_ctx->pg_database) global_ctx->pg_database = RedisModule_Strdup("postgres");
    if (!global_ctx->pg_user) global_ctx->pg_user = RedisModule_Strdup("postgres");
    if (!global_ctx->pg_password) global_ctx->pg_password = RedisModule_Strdup("");

    // Register commands
    if (RedisModule_CreateCommand(ctx, "pgcache.read", PGCRead_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.write", PGCWrite_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.invalidate", PGCInvalidate_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.multiread", PGCMultiRead_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Register new enhanced commands
    if (RedisModule_CreateCommand(ctx, "pgcache.setup.notifications", PGCSetupNotifications_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.setup.wal", PGCSetupWAL_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.setup.watchforwarding", PGCSetupWatchForwarding_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.status", PGCStatus_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.wal.list", PGCWALList_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.watch", PGCWatch_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Initialize features if enabled at startup
    if (global_ctx->enable_notifications) {
        setup_pg_notifications(global_ctx);
        if (global_ctx->notify_conn) {
            pthread_create(&global_ctx->notify_thread, NULL, notification_listener_thread, global_ctx);
        }
    }

    if (global_ctx->enable_wal) {
        setup_wal_reading(global_ctx);
        if (global_ctx->wal_conn) {
            pthread_create(&global_ctx->wal_thread, NULL, wal_reader_thread, global_ctx);
        }
    }

    if (global_ctx->enable_watch_forwarding) {
        setup_watch_forwarding(global_ctx);
    }

    RedisModule_Log(ctx, "notice", "PostgreSQL cache Redis module loaded with enhanced features");
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    if (global_ctx) {
        // Stop background threads
        if (global_ctx->notify_thread_running) {
            global_ctx->notify_thread_running = 0;
            pthread_join(global_ctx->notify_thread, NULL);
        }

        if (global_ctx->wal_thread_running) {
            global_ctx->wal_thread_running = 0;
            pthread_join(global_ctx->wal_thread, NULL);
        }

        // Close connections
        if (global_ctx->pg_conn) {
            PQfinish(global_ctx->pg_conn);
        }

        if (global_ctx->notify_conn) {
            PQfinish(global_ctx->notify_conn);
        }

        if (global_ctx->wal_conn) {
            PQfinish(global_ctx->wal_conn);
        }

        // Free memory
        RedisModule_Free(global_ctx->pg_host);
        RedisModule_Free(global_ctx->pg_database);
        RedisModule_Free(global_ctx->pg_user);
        RedisModule_Free(global_ctx->pg_password);
        RedisModule_Free(global_ctx->cache_prefix);
        RedisModule_Free(global_ctx->wal_slot_name);
        RedisModule_Free(global_ctx->wal_publication_name);

        pthread_mutex_destroy(&global_ctx->conn_mutex);
        RedisModule_Free(global_ctx);
    }

    return REDISMODULE_OK;
}
