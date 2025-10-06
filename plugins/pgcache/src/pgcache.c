#include <redismodule.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <jansson.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

/* Module metadata */
#define MODULE_NAME "pgcache"
#define MODULE_VERSION 2

/* Enhanced features */
#define MAX_CONNECTIONS 20
#define CONNECTION_IDLE_TIMEOUT 300  // 5 minutes
#define STREAM_MAX_LEN 10000
#define OUTBOX_STREAM "pgcache:outbox"
#define WATCH_STREAM "pgcache:watches"
#define METRICS_KEY "pgcache:metrics"

/* Connection pool entry */
typedef struct PGConnection {
    PGconn *conn;
    time_t last_used;
    int in_use;
    pthread_mutex_t conn_mutex;
} PGConnection;

/* Watch entry for cache invalidation */
typedef struct WatchEntry {
    char *table;
    char *key_pattern;
    char *channel;
    char *stream;
    time_t created_at;
    struct WatchEntry *next;
} WatchEntry;

/* Global state */
typedef struct PGCtx {
    // Basic configuration
    char *pg_host;
    int pg_port;
    char *pg_database;
    char *pg_user;
    char *pg_password;
    int default_ttl;
    char *cache_prefix;
    RedisModuleCtx *redis_ctx;

    // Enhanced connection pooling (PgBouncer-like)
    PGConnection *connection_pool;
    int pool_size;
    int active_connections;
    pthread_mutex_t pool_mutex;

    // Notification handling
    int enable_notifications;
    PGconn *notify_conn;
    pthread_t notify_thread;
    int notify_thread_running;
    WatchEntry *watch_list;
    pthread_mutex_t watch_mutex;

    // WAL reading and streaming
    int enable_wal;
    char *wal_slot_name;
    char *wal_publication_name;
    PGconn *wal_conn;
    pthread_t wal_thread;
    int wal_thread_running;

    // Watch forwarding to streams
    int enable_watch_forwarding;
    int enable_outbox;
    char *outbox_stream;
    char *watch_stream;

    // Performance monitoring
    long long cache_hits;
    long long cache_misses;
    long long notifications_received;
    long long wal_events_processed;
    long long queries_executed;
    pthread_mutex_t metrics_mutex;

    // Transaction support
    int in_transaction;
    char *current_transaction_id;
    pthread_mutex_t transaction_mutex;
} PGCtx;

static PGCtx *global_ctx = NULL;

/* Helper functions */

/* Initialize connection pool */
static int init_connection_pool(PGCtx *ctx) {
    pthread_mutex_lock(&ctx->pool_mutex);

    ctx->connection_pool = RedisModule_Alloc(sizeof(PGConnection) * ctx->pool_size);
    if (!ctx->connection_pool) {
        pthread_mutex_unlock(&ctx->pool_mutex);
        return -1;
    }

    memset(ctx->connection_pool, 0, sizeof(PGConnection) * ctx->pool_size);

    for (int i = 0; i < ctx->pool_size; i++) {
        pthread_mutex_init(&ctx->connection_pool[i].conn_mutex, NULL);
    }

    pthread_mutex_unlock(&ctx->pool_mutex);
    return 0;
}

/* Get connection from pool (PgBouncer-like) */
static PGconn* get_pooled_connection(PGCtx *ctx) {
    time_t now = time(NULL);
    pthread_mutex_lock(&ctx->pool_mutex);

    // First, try to find an available connection
    for (int i = 0; i < ctx->pool_size; i++) {
        if (!ctx->connection_pool[i].in_use &&
            ctx->connection_pool[i].conn &&
            PQstatus(ctx->connection_pool[i].conn) == CONNECTION_OK) {

            // Check if connection is too old
            if (now - ctx->connection_pool[i].last_used > CONNECTION_IDLE_TIMEOUT) {
                PQfinish(ctx->connection_pool[i].conn);
                ctx->connection_pool[i].conn = NULL;
            } else {
                pthread_mutex_lock(&ctx->connection_pool[i].conn_mutex);
                ctx->connection_pool[i].in_use = 1;
                ctx->connection_pool[i].last_used = now;
                pthread_mutex_unlock(&ctx->pool_mutex);
                return ctx->connection_pool[i].conn;
            }
        }
    }

    // Create new connection if pool not full
    if (ctx->active_connections < ctx->pool_size) {
        for (int i = 0; i < ctx->pool_size; i++) {
            if (ctx->connection_pool[i].conn == NULL) {
                char conninfo[512];
                snprintf(conninfo, sizeof(conninfo),
                        "host=%s port=%d dbname=%s user=%s password=%s",
                        ctx->pg_host, ctx->pg_port, ctx->pg_database,
                        ctx->pg_user, ctx->pg_password);

                ctx->connection_pool[i].conn = PQconnectdb(conninfo);
                ctx->active_connections++;

                if (PQstatus(ctx->connection_pool[i].conn) == CONNECTION_OK) {
                    pthread_mutex_lock(&ctx->connection_pool[i].conn_mutex);
                    ctx->connection_pool[i].in_use = 1;
                    ctx->connection_pool[i].last_used = now;
                    pthread_mutex_unlock(&ctx->pool_mutex);
                    return ctx->connection_pool[i].conn;
                } else {
                    RedisModule_Log(ctx->redis_ctx, "warning",
                                   "Failed to connect to PostgreSQL: %s",
                                   PQerrorMessage(ctx->connection_pool[i].conn));
                    PQfinish(ctx->connection_pool[i].conn);
                    ctx->connection_pool[i].conn = NULL;
                    ctx->active_connections--;
                }
            }
        }
    }

    pthread_mutex_unlock(&ctx->pool_mutex);

    // If we get here, no connection available
    RedisModule_Log(ctx->redis_ctx, "warning", "No available PostgreSQL connections in pool");
    return NULL;
}

/* Return connection to pool */
static void return_pooled_connection(PGCtx *ctx, PGconn *conn) {
    pthread_mutex_lock(&ctx->pool_mutex);

    for (int i = 0; i < ctx->pool_size; i++) {
        if (ctx->connection_pool[i].conn == conn) {
            ctx->connection_pool[i].in_use = 0;
            pthread_mutex_unlock(&ctx->connection_pool[i].conn_mutex);
            break;
        }
    }

    pthread_mutex_unlock(&ctx->pool_mutex);
}

static char* build_cache_key(PGCtx *ctx, const char *table, const char *primary_key_json) {
    size_t key_len = strlen(ctx->cache_prefix) + strlen(table) + strlen(primary_key_json) + 3;
    char *cache_key = RedisModule_Alloc(key_len);
    snprintf(cache_key, key_len, "%s%s:%s", ctx->cache_prefix, table, primary_key_json);
    return cache_key;
}

/* Add event to Redis stream */
static void add_to_stream(PGCtx *ctx, const char *stream, const char *event_type, const char *data) {
    if (!ctx->enable_outbox && strcmp(stream, ctx->outbox_stream) == 0) {
        return;
    }

    json_t *event = json_object();
    json_object_set_new(event, "type", json_string(event_type));
    json_object_set_new(event, "timestamp", json_real((double)time(NULL)));

    if (data) {
        json_object_set_new(event, "data", json_string(data));
    }

    char *event_str = json_dumps(event, JSON_COMPACT);

    // Add to Redis stream
    RedisModule_Call(ctx->redis_ctx, "XADD", "cccc",
                    stream, "*", "event", event_str);

    free(event_str);
    json_decref(event);
}

/* Update metrics */
static void update_metrics(PGCtx *ctx, const char *metric, long long delta) {
    pthread_mutex_lock(&ctx->metrics_mutex);

    if (strcmp(metric, "cache_hits") == 0) {
        ctx->cache_hits += delta;
    } else if (strcmp(metric, "cache_misses") == 0) {
        ctx->cache_misses += delta;
    } else if (strcmp(metric, "notifications_received") == 0) {
        ctx->notifications_received += delta;
    } else if (strcmp(metric, "wal_events_processed") == 0) {
        ctx->wal_events_processed += delta;
    } else if (strcmp(metric, "queries_executed") == 0) {
        ctx->queries_executed += delta;
    }

    pthread_mutex_unlock(&ctx->metrics_mutex);
}

/* Watch management functions */
static WatchEntry* create_watch_entry(const char *table, const char *key_pattern,
                                     const char *channel, const char *stream) {
    WatchEntry *entry = RedisModule_Alloc(sizeof(WatchEntry));
    entry->table = RedisModule_Strdup(table);
    entry->key_pattern = RedisModule_Strdup(key_pattern);
    entry->channel = RedisModule_Strdup(channel);
    entry->stream = RedisModule_Strdup(stream);
    entry->created_at = time(NULL);
    entry->next = NULL;
    return entry;
}

static void add_watch_entry(PGCtx *ctx, WatchEntry *entry) {
    pthread_mutex_lock(&ctx->watch_mutex);

    entry->next = ctx->watch_list;
    ctx->watch_list = entry;

    pthread_mutex_unlock(&ctx->watch_mutex);
}

static void remove_watch_entry(PGCtx *ctx, const char *table) {
    pthread_mutex_lock(&ctx->watch_mutex);

    WatchEntry *prev = NULL;
    WatchEntry *curr = ctx->watch_list;

    while (curr) {
        if (strcmp(curr->table, table) == 0) {
            if (prev) {
                prev->next = curr->next;
            } else {
                ctx->watch_list = curr->next;
            }

            RedisModule_Free(curr->table);
            RedisModule_Free(curr->key_pattern);
            RedisModule_Free(curr->channel);
            RedisModule_Free(curr->stream);
            RedisModule_Free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }

    pthread_mutex_unlock(&ctx->watch_mutex);
}

static WatchEntry* find_watch_entry(PGCtx *ctx, const char *table) {
    pthread_mutex_lock(&ctx->watch_mutex);

    WatchEntry *curr = ctx->watch_list;
    while (curr) {
        if (strcmp(curr->table, table) == 0) {
            pthread_mutex_unlock(&ctx->watch_mutex);
            return curr;
        }
        curr = curr->next;
    }

    pthread_mutex_unlock(&ctx->watch_mutex);
    return NULL;
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

    // Also add to outbox stream if enabled
    if (ctx->enable_outbox) {
        add_to_stream(ctx, ctx->outbox_stream, event_type, event_str);
    }

    free(event_str);
    json_decref(event);
}

static json_t* execute_pg_query(PGCtx *ctx, const char *query, int *row_count) {
    PGconn *conn = get_pooled_connection(ctx);
    if (!conn) {
        return NULL;
    }

    update_metrics(ctx, "queries_executed", 1);

    // Handle transaction state
    pthread_mutex_lock(&ctx->transaction_mutex);
    int is_transaction_query = (strncmp(query, "BEGIN", 5) == 0 ||
                               strncmp(query, "COMMIT", 6) == 0 ||
                               strncmp(query, "ROLLBACK", 8) == 0);

    if (is_transaction_query && strncmp(query, "BEGIN", 5) == 0) {
        ctx->in_transaction = 1;
        if (!ctx->current_transaction_id) {
            ctx->current_transaction_id = RedisModule_Strdup("tx_123456"); // Simple ID for demo
        }
    } else if (is_transaction_query && (strncmp(query, "COMMIT", 6) == 0 || strncmp(query, "ROLLBACK", 8) == 0)) {
        ctx->in_transaction = 0;
        if (ctx->current_transaction_id) {
            RedisModule_Free(ctx->current_transaction_id);
            ctx->current_transaction_id = NULL;
        }
    }
    pthread_mutex_unlock(&ctx->transaction_mutex);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK) {
        RedisModule_Log(ctx->redis_ctx, "warning",
                       "PostgreSQL query failed: %s", PQerrorMessage(conn));
        PQclear(res);
        return_pooled_connection(ctx, conn);
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
    return_pooled_connection(ctx, conn);
    return result_array;
}

/* Enhanced notification handling functions */
static void setup_pg_notifications(PGCtx *ctx) {
    if (!ctx->enable_notifications) return;

    char conninfo[512];
    snprintf(conninfo, sizeof(conninfo),
            "host=%s port=%d dbname=%s user=%s password=%s",
            ctx->pg_host, ctx->pg_port, ctx->pg_database,
            ctx->pg_user, ctx->pg_password);

    ctx->notify_conn = PQconnectdb(conninfo);
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

    RedisModule_Log(ctx->redis_ctx, "notice", "Enhanced notification listener thread started");

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

                    update_metrics(ctx, "notifications_received", 1);

                    // Build primary key from old_data or new_data
                    json_t *pk_data = old_data ? old_data : new_data;
                    if (pk_data && json_is_object(pk_data)) {
                        char *pk_json = json_dumps(pk_data, JSON_COMPACT);

                        // Invalidate cache entry
                        char *cache_key = build_cache_key(ctx, table, pk_json);

                        RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
                        RedisModule_Call(rctx, "DEL", "c", cache_key);
                        RedisModule_FreeThreadSafeContext(rctx);

                        // Publish invalidation event to pubsub
                        char event_data[1024];
                        snprintf(event_data, sizeof(event_data),
                                "{\"operation\":\"%s\",\"table\":\"%s\",\"key\":%s}",
                                operation, table, pk_json);

                        publish_event(ctx, "pg_invalidation", table, event_data);

                        // Forward to watch streams if configured
                        if (ctx->enable_watch_forwarding) {
                            WatchEntry *watch = find_watch_entry(ctx, table);
                            if (watch) {
                                // Add to watch stream
                                add_to_stream(ctx, watch->stream, "cache_invalidation",
                                            event_data);

                                // Publish to watch channel
                                RedisModule_Call(ctx->redis_ctx, "PUBLISH", "cc",
                                               watch->channel, event_data);
                            }
                        }

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
    RedisModule_Log(ctx->redis_ctx, "notice", "Enhanced notification listener thread stopped");
    return NULL;
}

/* Enhanced WAL reading functions */
static void setup_wal_reading(PGCtx *ctx) {
    if (!ctx->enable_wal) return;

    char conninfo[512];
    snprintf(conninfo, sizeof(conninfo),
            "host=%s port=%d dbname=%s user=%s password=%s replication=database",
            ctx->pg_host, ctx->pg_port, ctx->pg_database,
            ctx->pg_user, ctx->pg_password);

    // Create logical replication slot if it doesn't exist
    char create_slot_query[512];
    snprintf(create_slot_query, sizeof(create_slot_query),
             "SELECT * FROM pg_create_logical_replication_slot('%s', 'pgoutput')",
             ctx->wal_slot_name);

    PGconn *temp_conn = get_pooled_connection(ctx);
    if (temp_conn) {
        PGresult *res = PQexec(temp_conn, create_slot_query);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            RedisModule_Log(ctx->redis_ctx, "warning",
                           "Failed to create replication slot: %s", PQerrorMessage(temp_conn));
        }
        PQclear(res);
        return_pooled_connection(ctx, temp_conn);
    }

    // Connect for WAL streaming
    ctx->wal_conn = PQconnectdb(conninfo);
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

    RedisModule_Log(ctx->redis_ctx, "notice", "Enhanced WAL reading enabled");
}

static void* wal_reader_thread(void *arg) {
    PGCtx *ctx = (PGCtx*)arg;
    ctx->wal_thread_running = 1;

    RedisModule_Log(ctx->redis_ctx, "notice", "Enhanced WAL reader thread started");

    while (ctx->wal_thread_running && ctx->wal_conn) {
        // Read WAL data
        char *buffer = NULL;
        int len = PQgetCopyData(ctx->wal_conn, &buffer, 0);

        if (len > 0) {
            update_metrics(ctx, "wal_events_processed", 1);

            // Process WAL message - basic parsing for demo
            // Real implementation would decode the WAL format properly
            char wal_info[1024];
            snprintf(wal_info, sizeof(wal_info),
                    "{\"size\":%d,\"timestamp\":%lld}", len, (long long)time(NULL));

            // Publish WAL event to pubsub
            publish_event(ctx, "wal_change", "system", wal_info);

            // Add to outbox stream if enabled
            if (ctx->enable_outbox) {
                add_to_stream(ctx, ctx->outbox_stream, "wal_change", wal_info);
            }

            RedisModule_Log(ctx->redis_ctx, "notice", "Processed WAL data: %d bytes", len);

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
    RedisModule_Log(ctx->redis_ctx, "notice", "Enhanced WAL reader thread stopped");
    return NULL;
}

/* Enhanced watch forwarding functions */
static void setup_watch_forwarding(PGCtx *ctx) {
    if (!ctx->enable_watch_forwarding) return;

    // Create default streams if they don't exist
    RedisModule_Call(ctx->redis_ctx, "XTRIM", "ccc", ctx->watch_stream, "MAXLEN", "~", STREAM_MAX_LEN);

    RedisModule_Log(ctx->redis_ctx, "notice", "Enhanced watch forwarding enabled");
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
        update_metrics(pgctx, "cache_hits", 1);
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

    // Cache miss
    update_metrics(pgctx, "cache_misses", 1);
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
    json_object_set_new(status, "outbox_enabled", json_boolean(pgctx->enable_outbox));
    json_object_set_new(status, "pool_size", json_integer(pgctx->pool_size));
    json_object_set_new(status, "active_connections", json_integer(pgctx->active_connections));
    json_object_set_new(status, "cache_prefix", json_string(pgctx->cache_prefix));
    json_object_set_new(status, "default_ttl", json_integer(pgctx->default_ttl));
    json_object_set_new(status, "outbox_stream", json_string(pgctx->outbox_stream));
    json_object_set_new(status, "watch_stream", json_string(pgctx->watch_stream));
    json_object_set_new(status, "version", json_integer(MODULE_VERSION));

    // Transaction status
    pthread_mutex_lock(&pgctx->transaction_mutex);
    json_object_set_new(status, "in_transaction", json_boolean(pgctx->in_transaction));
    json_object_set_new(status, "transaction_id", pgctx->current_transaction_id ?
                       json_string(pgctx->current_transaction_id) : json_null());
    pthread_mutex_unlock(&pgctx->transaction_mutex);

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

    // Add to watch stream
    add_to_stream(pgctx, pgctx->watch_stream, "watch_started", event_str);

    free(event_str);
    json_decref(watch_event);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* PGCACHE.SETUP.OUTBOX <enable> */
int PGCSetupOutbox_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *enable_str = RedisModule_StringPtrLen(argv[1], NULL);
    pgctx->enable_outbox = strcmp(enable_str, "1") == 0 ||
                          strcmp(enable_str, "true") == 0 ||
                          strcmp(enable_str, "yes") == 0;

    if (pgctx->enable_outbox) {
        // Initialize outbox stream
        RedisModule_Call(ctx, "XTRIM", "ccc", pgctx->outbox_stream, "MAXLEN", "~", STREAM_MAX_LEN);
        RedisModule_Log(ctx, "notice", "Outbox streams enabled");
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* PGCACHE.SETUP.CONNECTIONPOOL <pool_size> */
int PGCSetupConnectionPool_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    int pool_size = atoi(RedisModule_StringPtrLen(argv[1], NULL));
    if (pool_size < 1 || pool_size > MAX_CONNECTIONS) {
        return RedisModule_ReplyWithError(ctx, "Pool size must be between 1 and 20");
    }

    pgctx->pool_size = pool_size;

    if (init_connection_pool(pgctx) == 0) {
        RedisModule_Log(ctx, "notice", "Connection pool initialized with size %d", pool_size);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        RedisModule_ReplyWithError(ctx, "Failed to initialize connection pool");
    }

    return REDISMODULE_OK;
}

/* PGCACHE.WATCH.ADD <table> <key_pattern> <channel> <stream> */
int PGCWatchAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 5) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *key_pattern = RedisModule_StringPtrLen(argv[2], NULL);
    const char *channel = RedisModule_StringPtrLen(argv[3], NULL);
    const char *stream = RedisModule_StringPtrLen(argv[4], NULL);

    WatchEntry *entry = create_watch_entry(table, key_pattern, channel, stream);
    add_watch_entry(pgctx, entry);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* PGCACHE.WATCH.REMOVE <table> */
int PGCWatchRemove_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    remove_watch_entry(pgctx, table);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* PGCACHE.WATCH.LIST */
int PGCWatchList_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    json_t *watches_array = json_array();
    pthread_mutex_lock(&pgctx->watch_mutex);

    WatchEntry *curr = pgctx->watch_list;
    while (curr) {
        json_t *watch_obj = json_object();
        json_object_set_new(watch_obj, "table", json_string(curr->table));
        json_object_set_new(watch_obj, "key_pattern", json_string(curr->key_pattern));
        json_object_set_new(watch_obj, "channel", json_string(curr->channel));
        json_object_set_new(watch_obj, "stream", json_string(curr->stream));
        json_object_set_new(watch_obj, "created_at", json_integer(curr->created_at));

        json_array_append_new(watches_array, watch_obj);
        curr = curr->next;
    }

    pthread_mutex_unlock(&pgctx->watch_mutex);

    char *watches_str = json_dumps(watches_array, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, watches_str, strlen(watches_str));

    free(watches_str);
    json_decref(watches_array);
    return REDISMODULE_OK;
}

/* PGCACHE.STREAMS.INFO */
int PGCStreamsInfo_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    json_t *streams_info = json_object();

    // Outbox stream info
    RedisModuleCallReply *outbox_reply = RedisModule_Call(ctx, "XLEN", "c", pgctx->outbox_stream);
    if (RedisModule_CallReplyType(outbox_reply) == REDISMODULE_REPLY_INTEGER) {
        long long outbox_len = RedisModule_CallReplyInteger(outbox_reply);
        json_object_set_new(streams_info, "outbox_length", json_integer(outbox_len));
    }
    RedisModule_FreeCallReply(outbox_reply);

    // Watch stream info
    RedisModuleCallReply *watch_reply = RedisModule_Call(ctx, "XLEN", "c", pgctx->watch_stream);
    if (RedisModule_CallReplyType(watch_reply) == REDISMODULE_REPLY_INTEGER) {
        long long watch_len = RedisModule_CallReplyInteger(watch_reply);
        json_object_set_new(streams_info, "watch_length", json_integer(watch_len));
    }
    RedisModule_FreeCallReply(watch_reply);

    json_object_set_new(streams_info, "outbox_enabled", json_boolean(pgctx->enable_outbox));
    json_object_set_new(streams_info, "watch_forwarding_enabled", json_boolean(pgctx->enable_watch_forwarding));

    char *info_str = json_dumps(streams_info, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, info_str, strlen(info_str));

    free(info_str);
    json_decref(streams_info);
    return REDISMODULE_OK;
}

/* PGCACHE.METRICS */
int PGCMetrics_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    pthread_mutex_lock(&pgctx->metrics_mutex);

    json_t *metrics = json_object();
    json_object_set_new(metrics, "cache_hits", json_integer(pgctx->cache_hits));
    json_object_set_new(metrics, "cache_misses", json_integer(pgctx->cache_misses));
    json_object_set_new(metrics, "notifications_received", json_integer(pgctx->notifications_received));
    json_object_set_new(metrics, "wal_events_processed", json_integer(pgctx->wal_events_processed));
    json_object_set_new(metrics, "queries_executed", json_integer(pgctx->queries_executed));

    // Calculate hit rate
    long long total_requests = pgctx->cache_hits + pgctx->cache_misses;
    double hit_rate = total_requests > 0 ? (double)pgctx->cache_hits / total_requests * 100.0 : 0.0;
    json_object_set_new(metrics, "hit_rate_percent", json_real(hit_rate));

    pthread_mutex_unlock(&pgctx->metrics_mutex);

    char *metrics_str = json_dumps(metrics, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, metrics_str, strlen(metrics_str));

    free(metrics_str);
    json_decref(metrics);
    return REDISMODULE_OK;
}

/* PGCACHE.TRANSACTION.BEGIN */
int PGCTransactionBegin_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    pthread_mutex_lock(&pgctx->transaction_mutex);

    if (pgctx->in_transaction) {
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        return RedisModule_ReplyWithError(ctx, "Transaction already in progress");
    }

    int row_count = 0;
    json_t *result = execute_pg_query(pgctx, "BEGIN", &row_count);

    if (result) {
        json_decref(result);
        pgctx->in_transaction = 1;
        if (!pgctx->current_transaction_id) {
            pgctx->current_transaction_id = RedisModule_Strdup("tx_123456");
        }
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        RedisModule_ReplyWithError(ctx, "Failed to begin transaction");
    }

    return REDISMODULE_OK;
}

/* PGCACHE.TRANSACTION.COMMIT */
int PGCTransactionCommit_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    pthread_mutex_lock(&pgctx->transaction_mutex);

    if (!pgctx->in_transaction) {
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        return RedisModule_ReplyWithError(ctx, "No transaction in progress");
    }

    int row_count = 0;
    json_t *result = execute_pg_query(pgctx, "COMMIT", &row_count);

    if (result) {
        json_decref(result);
        pgctx->in_transaction = 0;
        if (pgctx->current_transaction_id) {
            RedisModule_Free(pgctx->current_transaction_id);
            pgctx->current_transaction_id = NULL;
        }
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        RedisModule_ReplyWithError(ctx, "Failed to commit transaction");
    }

    return REDISMODULE_OK;
}

/* PGCACHE.TRANSACTION.ROLLBACK */
int PGCTransactionRollback_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    pthread_mutex_lock(&pgctx->transaction_mutex);

    if (!pgctx->in_transaction) {
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        return RedisModule_ReplyWithError(ctx, "No transaction in progress");
    }

    int row_count = 0;
    json_t *result = execute_pg_query(pgctx, "ROLLBACK", &row_count);

    if (result) {
        json_decref(result);
        pgctx->in_transaction = 0;
        if (pgctx->current_transaction_id) {
            RedisModule_Free(pgctx->current_transaction_id);
            pgctx->current_transaction_id = NULL;
        }
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        pthread_mutex_unlock(&pgctx->transaction_mutex);
        RedisModule_ReplyWithError(ctx, "Failed to rollback transaction");
    }

    return REDISMODULE_OK;
}

/* PGCACHE.TRANSACTION.STATUS */
int PGCTransactionStatus_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    pthread_mutex_lock(&pgctx->transaction_mutex);

    json_t *status = json_object();
    json_object_set_new(status, "in_transaction", json_boolean(pgctx->in_transaction));
    json_object_set_new(status, "transaction_id", pgctx->current_transaction_id ?
                       json_string(pgctx->current_transaction_id) : json_null());

    pthread_mutex_unlock(&pgctx->transaction_mutex);

    char *status_str = json_dumps(status, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, status_str, strlen(status_str));

    free(status_str);
    json_decref(status);
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
    global_ctx->pool_size = 5; // Default pool size

    // Initialize new fields
    global_ctx->enable_notifications = 0;
    global_ctx->notify_thread_running = 0;
    global_ctx->enable_wal = 0;
    global_ctx->wal_thread_running = 0;
    global_ctx->enable_watch_forwarding = 0;
    global_ctx->enable_outbox = 0;
    global_ctx->wal_slot_name = RedisModule_Strdup("pgcache_slot");
    global_ctx->wal_publication_name = RedisModule_Strdup("pgcache_pub");
    global_ctx->outbox_stream = RedisModule_Strdup(OUTBOX_STREAM);
    global_ctx->watch_stream = RedisModule_Strdup(WATCH_STREAM);
    global_ctx->current_transaction_id = NULL;

    pthread_mutex_init(&global_ctx->pool_mutex, NULL);
    pthread_mutex_init(&global_ctx->watch_mutex, NULL);
    pthread_mutex_init(&global_ctx->metrics_mutex, NULL);
    pthread_mutex_init(&global_ctx->transaction_mutex, NULL);

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
        } else if (strcmp(param, "enable_outbox") == 0) {
            global_ctx->enable_outbox = strcmp(value, "1") == 0 ||
                                       strcmp(value, "true") == 0 ||
                                       strcmp(value, "yes") == 0;
        } else if (strcmp(param, "pool_size") == 0) {
            global_ctx->pool_size = atoi(value);
            if (global_ctx->pool_size < 1 || global_ctx->pool_size > MAX_CONNECTIONS) {
                global_ctx->pool_size = 5;
            }
        } else if (strcmp(param, "outbox_stream") == 0) {
            RedisModule_Free(global_ctx->outbox_stream);
            global_ctx->outbox_stream = RedisModule_Strdup(value);
        } else if (strcmp(param, "watch_stream") == 0) {
            RedisModule_Free(global_ctx->watch_stream);
            global_ctx->watch_stream = RedisModule_Strdup(value);
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

    // Initialize connection pool
    if (init_connection_pool(global_ctx) != 0) {
        RedisModule_Log(ctx, "warning", "Failed to initialize connection pool");
    }

    if (global_ctx->enable_watch_forwarding) {
        setup_watch_forwarding(global_ctx);
    }

    if (global_ctx->enable_outbox) {
        RedisModule_Call(ctx, "XTRIM", "ccc", global_ctx->outbox_stream, "MAXLEN", "~", STREAM_MAX_LEN);
    }

    // Register enhanced commands
    if (RedisModule_CreateCommand(ctx, "pgcache.setup.outbox", PGCSetupOutbox_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.setup.connectionpool", PGCSetupConnectionPool_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.watch.add", PGCWatchAdd_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.watch.remove", PGCWatchRemove_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.watch.list", PGCWatchList_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.streams.info", PGCStreamsInfo_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.metrics", PGCMetrics_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.transaction.begin", PGCTransactionBegin_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.transaction.commit", PGCTransactionCommit_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.transaction.rollback", PGCTransactionRollback_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.transaction.status", PGCTransactionStatus_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    RedisModule_Log(ctx, "notice", "PostgreSQL cache Redis module loaded with enhanced features v%d", MODULE_VERSION);
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

        // Close connections and cleanup connection pool
        if (global_ctx->connection_pool) {
            for (int i = 0; i < global_ctx->pool_size; i++) {
                if (global_ctx->connection_pool[i].conn) {
                    PQfinish(global_ctx->connection_pool[i].conn);
                }
                pthread_mutex_destroy(&global_ctx->connection_pool[i].conn_mutex);
            }
            RedisModule_Free(global_ctx->connection_pool);
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
        RedisModule_Free(global_ctx->outbox_stream);
        RedisModule_Free(global_ctx->watch_stream);
        RedisModule_Free(global_ctx->current_transaction_id);

        // Cleanup watch list
        WatchEntry *curr = global_ctx->watch_list;
        while (curr) {
            WatchEntry *next = curr->next;
            RedisModule_Free(curr->table);
            RedisModule_Free(curr->key_pattern);
            RedisModule_Free(curr->channel);
            RedisModule_Free(curr->stream);
            RedisModule_Free(curr);
            curr = next;
        }

        pthread_mutex_destroy(&global_ctx->pool_mutex);
        pthread_mutex_destroy(&global_ctx->watch_mutex);
        pthread_mutex_destroy(&global_ctx->metrics_mutex);
        pthread_mutex_destroy(&global_ctx->transaction_mutex);
        RedisModule_Free(global_ctx);
    }

    return REDISMODULE_OK;
}
