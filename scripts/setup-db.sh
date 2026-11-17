#!/usr/bin/env bash
set -e
set -a; source .env; set +a

# Databases as reached from where the client runs (host psql/sqlcmd, or docker exec
# into the container): localhost works for both via the published ports. Docker
# service names (postgres, sqlserver) only resolve inside the compose network.
# Override in .env for the remote databases.
MSSQL_HOST=${MSSQL_HOST:-localhost}
MSSQL_PORT=${MSSQL_PORT:-1433}
PG_HOST=${PG_HOST:-localhost}
PG_PORT=${PG_PORT:-5432}
PG_DB=${PG_DB:-bpjsk_poc}
# Owner of the bpjs_* tables; needs to create roles, publications and slots.
PG_ADMIN_USER=${PG_ADMIN_USER:-postgres}
# Logins the JDBC sinks write with (same defaults as docker-compose.yaml). The
# loop filter drops every change these logins make, so apps must not use them.
MSSQL_SINK_USER=${MSSQL_SINK_USER:-cdc_sink}
MSSQL_SINK_PASSWORD=${MSSQL_SINK_PASSWORD:-$MSSQL_PASSWORD}
PG_SINK_USER=${PG_SINK_USER:-cdc_sink}
PG_SINK_PASSWORD=${PG_SINK_PASSWORD:-$PG_PASSWORD}

# sqlcmd: use a local install, else borrow the one in the local sqlserver container.
if command -v sqlcmd >/dev/null; then
  SQLCMD=(sqlcmd)
else
  SQLCMD=(docker exec -i sqlserver "$(docker exec sqlserver sh -c 'ls /opt/mssql-tools18/bin/sqlcmd 2>/dev/null || ls /opt/mssql-tools/bin/sqlcmd')")
fi
run_mssql() {
  "${SQLCMD[@]}" -S "$MSSQL_HOST,$MSSQL_PORT" -U sa -P "$MSSQL_PASSWORD" -C -I -b "${@:2}" -i /dev/stdin < "$1"
}

# psql: use a local install, else the one in the local postgres container.
if command -v psql >/dev/null; then
  PSQL=(psql)
else
  PSQL=(docker exec -i -e PGPASSWORD postgres psql)
fi
export PGPASSWORD="$PG_PASSWORD"
run_pg() {
  "${PSQL[@]}" -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 "${@:2}" < "$1"
}

echo "== SQL Server ($MSSQL_HOST:$MSSQL_PORT) =="
run_mssql sql/mssql-01-schema.sql
run_mssql sql/mssql-02-enable-cdc.sql
run_mssql sql/mssql-03-lww-trigger.sql
run_mssql sql/mssql-05-sink-login.sql -v SINK_USER="$MSSQL_SINK_USER" SINK_PW="$MSSQL_SINK_PASSWORD"

echo "== PostgreSQL ($PG_HOST:$PG_PORT/$PG_DB) =="
run_pg sql/postgres-01-schema.sql
run_pg sql/postgres-02-replication.sql -v pw="$PG_PASSWORD"
run_pg sql/postgres-03-lww-trigger.sql
run_pg sql/postgres-05-sink-role.sql -v sink_user="$PG_SINK_USER" -v sink_pw="$PG_SINK_PASSWORD"

echo "Done."
