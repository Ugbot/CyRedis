# PostgreSQL Cache Redis Module

A Redis module that provides read-through caching directly from PostgreSQL with real-time invalidation capabilities. The module runs server-side in Redis and handles all database queries internally, eliminating network round-trips on cache misses.

## Features

- **Read-Through Caching**: Automatically queries PostgreSQL on cache misses
- **Server-Side Logic**: All caching logic runs inside Redis
- **High Performance**: No client-side database queries
- **Multi-Key Operations**: Efficient batch operations
- **Real-Time Events**: Publishes cache events to `pg_cache_events` pubsub channel
- **Configurable**: Customizable TTL, prefixes, and database connections

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │───▶│    Redis    │───▶│ PostgreSQL  │
│             │    │   Module    │    │  Database   │
└─────────────┘    └─────────────┘    └─────────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │  PubSub     │
                   │  Events     │
                   │pg_cache_evt │
                   └─────────────┘
```

## Building

### Prerequisites

- Redis server (6.0+ recommended)
- PostgreSQL client library `libpq` and `pg_config` on PATH
  (`postgresql-server-dev-all` on Ubuntu, `brew install postgresql` on macOS)
- Jansson JSON library (`libjansson-dev`)
- A C compiler (gcc/clang)

The module links against libpq directly (`#include <libpq-fe.h>`) for PostgreSQL
access and jansson for JSON.

### Vendored Dependencies

- **psqlodbc**: vendored as a git submodule at `plugins/pgcache/psqlodbc`; the
  build script checks it out. (The module itself talks to PostgreSQL via libpq.)

### Build Steps

```bash
# Navigate to the module directory
cd plugins/pgcache

# Initialize submodules (if not already done)
git submodule update --init --recursive

# Build the module (checks for pg_config and jansson, then compiles pgcache.so)
./build_module.sh
```

### Building with Vendored Dependencies

The vendored psqlodbc library can be built separately if needed:

```bash
# Build psqlodbc
cd psqlodbc
./bootstrap
./configure
make
```

## Loading the Module

Start Redis with the module loaded:

```bash
redis-server --loadmodule ./pgcache.so \
    pg_host localhost \
    pg_database myapp \
    pg_user postgres \
    pg_password secret \
    default_ttl 3600 \
    cache_prefix pg_cache:
```

### Module Arguments

- `pg_host <host>`: PostgreSQL host (default: localhost)
- `pg_port <port>`: PostgreSQL port (default: 5432)
- `pg_database <db>`: PostgreSQL database (default: postgres)
- `pg_user <user>`: PostgreSQL user (default: postgres)
- `pg_password <pass>`: PostgreSQL password (default: empty)
- `default_ttl <seconds>`: Default cache TTL (default: 3600)
- `cache_prefix <prefix>`: Cache key prefix (default: pg_cache:)

## Redis Commands

### PGCACHE.READ

Read-through cache operation.

```
PGCACHE.READ <table> <primary_key_json> [TTL]
```

**Arguments:**
- `table`: PostgreSQL table name
- `primary_key_json`: JSON object with primary key columns
- `TTL`: Optional TTL override

**Returns:** JSON string of cached/query result, or nil if not found

**Example:**
```redis
PGCACHE.READ users {"id": 123}
PGCACHE.READ products {"category": "electronics", "id": 456} 1800
```

### PGCACHE.WRITE

Manually write to cache.

```
PGCACHE.WRITE <table> <primary_key_json> <data_json> [TTL]
```

**Arguments:**
- `table`: PostgreSQL table name
- `primary_key_json`: JSON object with primary key columns
- `data_json`: JSON data to cache
- `TTL`: Optional TTL override

**Returns:** OK

**Example:**
```redis
PGCACHE.WRITE users {"id": 123} {"name": "John", "email": "john@example.com"}
```

### PGCACHE.INVALIDATE

Invalidate cache entry.

```
PGCACHE.INVALIDATE <table> <primary_key_json>
```

**Arguments:**
- `table`: PostgreSQL table name
- `primary_key_json`: JSON object with primary key columns

**Returns:** OK

**Example:**
```redis
PGCACHE.INVALIDATE users {"id": 123}
```

### PGCACHE.MULTIREAD

Multi-key read-through operation.

```
PGCACHE.MULTIREAD <table> <primary_keys_json_array> [TTL]
```

**Arguments:**
- `table`: PostgreSQL table name
- `primary_keys_json_array`: JSON array of primary key objects
- `TTL`: Optional TTL override

**Returns:** JSON array of results

**Example:**
```redis
PGCACHE.MULTIREAD users [{"id": 1}, {"id": 2}, {"id": 3}]
```

## Usage Examples

### Basic Read-Through Caching

```python
from cy_redis import CyRedisClient

# Connect to the Redis instance that has the module loaded.
# execute_command takes a single list of arguments.
r = CyRedisClient(host="localhost", port=6379)

# First request - cache miss, module queries PostgreSQL
user = r.execute_command(['PGCACHE.READ', 'users', '{"id": 123}'])
print(user)  # {"name": "John", "email": "john@example.com"}

# Second request - cache hit
user = r.execute_command(['PGCACHE.READ', 'users', '{"id": 123}'])
print(user)  # Same result, from cache
```

### Multi-Key Operations

```python
# Get multiple users at once
users = r.execute_command(['PGCACHE.MULTIREAD', 'users',
                          '[{"id": 1}, {"id": 2}, {"id": 3}]'])
print(users)  # [{"name": "Alice", ...}, {"name": "Bob", ...}, ...]
```

### Manual Cache Management

```python
# Write to cache manually
r.execute_command(['PGCACHE.WRITE', 'users', '{"id": 999}',
                  '{"name": "New User", "email": "new@example.com"}'])

# Invalidate cache entry
r.execute_command(['PGCACHE.INVALIDATE', 'users', '{"id": 999}'])
```

## Events and Monitoring

The module publishes events to Redis pubsub channels:

- `pg_cache_events`: General cache events
- Events include: `cache_hit`, `cache_miss`, `cache_write`, `cache_invalidate`

```python
import asyncio, json
from cy_redis import CyRedisClient
from cy_redis.utils import RedisPubSubIterator

async def watch_cache_events():
    client = CyRedisClient(host="localhost", port=6379)
    async for message in RedisPubSubIterator(client, ["pg_cache_events"]):
        if message and message.get("type") == "message":
            event = json.loads(message["data"])
            print(f"Cache event: {event['type']} on {event['table']}")

asyncio.run(watch_cache_events())
```

## PostgreSQL Setup

### Required Tables

The module works with any PostgreSQL table structure. Primary keys are specified as JSON objects:

```sql
-- Example user table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(200),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Example product table with composite primary key
CREATE TABLE products (
    category VARCHAR(50),
    product_id INTEGER,
    name VARCHAR(200),
    price DECIMAL(10,2),
    PRIMARY KEY (category, product_id)
);
```

### Auto-Invalidation Triggers

Set up PostgreSQL triggers to automatically invalidate cache on data changes:

```sql
-- Function to notify cache invalidation
CREATE OR REPLACE FUNCTION notify_cache_invalidation()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('cache_invalidation',
        json_build_object(
            'table', TG_TABLE_NAME,
            'operation', TG_OP,
            'old_data', row_to_json(OLD),
            'new_data', row_to_json(NEW)
        )::text
    );
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Trigger for users table
CREATE TRIGGER users_cache_trigger
    AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION notify_cache_invalidation();
```

## Performance Considerations

### Connection Management

The module maintains persistent PostgreSQL connections to avoid connection overhead. Connection pooling is handled internally.

### Query Optimization

- Use appropriate indexes on primary key columns
- Consider table partitioning for large datasets
- Monitor query performance with PostgreSQL's `EXPLAIN`

### Memory Management

- Set appropriate TTL values to prevent memory bloat
- Monitor Redis memory usage
- Consider Redis memory limits for large datasets

## Troubleshooting

### Common Issues

1. **Module fails to load**
   - Check that all dependencies are installed
   - Verify Redis version compatibility
   - Check module build for errors

2. **PostgreSQL connection fails**
   - Verify PostgreSQL credentials
   - Check network connectivity
   - Ensure PostgreSQL accepts connections

3. **Queries return null**
   - Verify table and column names
   - Check primary key JSON format
   - Examine PostgreSQL logs for query errors

### Debugging

Enable Redis logging to see module activity:

```redis
CONFIG SET loglevel debug
```

Check module information:

```redis
MODULE LIST
INFO modules
```

## Automatic Cache Invalidation via WAL / CDC

The current module is synchronous and request-driven: cache entries are invalidated
explicitly with `PGCACHE.INVALIDATE` or expire via TTL.  For applications that need
the cache to stay coherent with live PostgreSQL writes without any application-side
invalidation calls, the natural extension is Change Data Capture (CDC) from the
PostgreSQL Write-Ahead Log or MySQL binary log.

### Two implementation approaches

**Option A — Standalone CDC daemon (recommended)**

A separate process tails the WAL using PostgreSQL logical replication
(`wal2json` output plugin) and calls `PGCACHE.INVALIDATE` whenever a row changes.
This is the simpler and more operationally predictable path:

```
PostgreSQL WAL → CDC daemon → PGCACHE.INVALIDATE → Redis cache invalidated
                           └→ XADD cy:cdc:{table} → Redis Stream (event log)
```

Set up a logical replication slot and publication in PostgreSQL:

```sql
SELECT pg_create_logical_replication_slot('pgcache_slot', 'wal2json');
CREATE PUBLICATION pgcache_pub FOR TABLE users, products;
```

The daemon consumes the slot and for each row-change event calls:

```
PGCACHE.INVALIDATE <table> <primary_key_json>
```

This keeps the CDC consumer decoupled from the Redis module lifecycle and means
a crashing consumer never affects Redis or PostgreSQL.

**Option B — Background thread inside the module**

A background thread (spawned in `RedisModule_OnLoad`) opens a second libpq
connection in logical-replication mode and applies invalidations directly inside
the Redis process.  This removes the external process dependency but couples the
WAL consumer's lifetime to Redis:

- Replication connection is held open inside the Redis process
- A PostgreSQL reconnect stall blocks the background thread (not the event loop)
- The replication slot must be dropped and recreated on module unload/reload

This approach makes sense when you want the CDC consumer to share the module's
Redis memory and key-space access without any network hops.

### MySQL binary log

The same two options apply for MySQL, using the MySQL C API's `mysql_binlog_*`
functions (or the higher-level `mysql-binlog-connector-c` library) in place of
libpq's replication interface.  The standalone-daemon approach is even more
strongly preferred for MySQL since the binlog protocol is more complex and
reconnection handling is more fragile inside a Redis module thread.

### Using existing CDC infrastructure

If you already have a CDC pipeline (Debezium, Maxwell, AWS DMS, etc.) publishing
change events, you can wire it to pgcache with a single consumer that translates
each event into a `PGCACHE.INVALIDATE` call — no module changes required.

## License

MIT License - see LICENSE file for details.
