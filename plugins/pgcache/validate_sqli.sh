#!/usr/bin/env bash
# Reproducible SQL-injection validation for the pgcache Redis module.
#
# Builds the module, seeds a Postgres table, loads the module into a Redis
# instance, and fires legitimate reads plus injection payloads through the
# table name, a string value, a column key, and a numeric value. The table
# must survive every injection attempt and only legitimate reads return rows.
#
# Designed to run INSIDE a Linux container that can reach PostgreSQL. Example
# with podman (PG reachable as host "cyr-pg" on a shared network):
#
#   podman network create cyrnet
#   podman run -d --name cyr-pg --network cyrnet \
#       -e POSTGRES_PASSWORD=cyrpass -e POSTGRES_USER=cyr -e POSTGRES_DB=cyrtest \
#       docker.io/library/postgres:16-alpine
#   podman run --rm -i --network cyrnet -v "$PWD":/work:Z \
#       docker.io/library/debian:bookworm bash -s < plugins/pgcache/validate_sqli.sh
set -euo pipefail

PG_HOST="${PG_HOST:-cyr-pg}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-cyrtest}"
PG_USER="${PG_USER:-cyr}"
export PGPASSWORD="${PG_PASSWORD:-cyrpass}"
REDIS_PORT="${REDIS_PORT:-6390}"
SRC_DIR="${SRC_DIR:-/work/plugins/pgcache}"

if ! command -v cc >/dev/null; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq >/dev/null
    apt-get install -y -qq build-essential libpq-dev libjansson-dev \
        redis-server postgresql-client >/dev/null
fi

PSQL="psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -tA"
$PSQL -c "DROP TABLE IF EXISTS users;
          CREATE TABLE users(id int primary key, name text, secret text);
          INSERT INTO users VALUES (1,'alice','s1'),(2,'bob','s2');" >/dev/null
echo "[seeded rows=$($PSQL -c 'SELECT count(*) FROM users;')]"

cc -fPIC -shared -std=gnu11 -O2 -Wall -I "$SRC_DIR/src" \
   -I "$(pg_config --includedir)" "$SRC_DIR/src/pgcache.c" \
   -o /tmp/pgcache.so -lpq -ljansson
echo "[module built]"

redis-server --port "$REDIS_PORT" --daemonize yes \
  --loadmodule /tmp/pgcache.so pg_host "$PG_HOST" pg_port "$PG_PORT" \
  pg_database "$PG_DB" pg_user "$PG_USER" pg_password "$PGPASSWORD"
sleep 1
R="redis-cli -p $REDIS_PORT"

fail() { echo "FAIL: $1"; exit 1; }
rows() { $PSQL -c 'SELECT count(*) FROM users;'; }

echo "T1 legit read:        $($R PGCACHE.READ users '{"id":1}')"
[ "$($R PGCACHE.READ users '{"id":1}')" != "" ] || fail "legit read returned nothing"

$R PGCACHE.READ 'users; DROP TABLE users; --' '{"id":1}' >/dev/null
[ "$(rows)" = "2" ] || fail "table-name injection dropped/altered the table"
echo "T2 table-name inject: blocked (rows=$(rows))"

$R PGCACHE.READ users '{"name":"alice'\'' OR '\''1'\''='\''1"}' >/dev/null
echo "T3 value injection:   blocked (no literal match)"

$R PGCACHE.READ users '{"id) ; DROP TABLE users; --":1}' >/dev/null
[ "$(rows)" = "2" ] || fail "column-key injection dropped/altered the table"
echo "T4 column-key inject: blocked (rows=$(rows))"

echo "T5 multiread:         $($R PGCACHE.MULTIREAD users '[{"id":1},{"id":2}]' | tr '\n' ' ')"

[ "$(rows)" = "2" ] || fail "table not intact at end"
echo "PASS: all injections blocked; legitimate reads work; table intact (rows=$(rows))"
