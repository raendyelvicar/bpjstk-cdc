#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
. scripts/env.sh
check_sink_logins
# Hosts, ports and logins come from .env.$APP_ENV via scripts/env.sh (MSSQL_HOST, PG_HOST,
# MSSQL_ADMIN_USER, PG_ADMIN_USER, *_SINK_USER ...). Run from a machine that can
# reach the databases; the admins must be able to enable CDC and create logins.
# debezium_replication gets PG_USER_PASSWORD when it is the source login (PG_USER).

# sqlcmd: use a local install, else borrow the one in the local sqlserver container.
if command -v sqlcmd >/dev/null; then
  SQLCMD=(sqlcmd)
else
  SQLCMD=(docker exec -i sqlserver "$(docker exec sqlserver sh -c 'ls /opt/mssql-tools18/bin/sqlcmd 2>/dev/null || ls /opt/mssql-tools/bin/sqlcmd')")
fi
run_mssql() {
  "${SQLCMD[@]}" -S "$MSSQL_HOST,$MSSQL_PORT" -U "$MSSQL_ADMIN_USER" -P "$MSSQL_PASSWORD" -C -I -b "${@:2}" -i /dev/stdin < "$1"
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
if [ "$PG_USER" = debezium_replication ]; then
  run_pg sql/postgres-02-replication.sql -v pw="$PG_USER_PASSWORD"
else
  run_pg sql/postgres-02-replication.sql   # source logs in as $PG_USER; leave the role's password alone
fi
run_pg sql/postgres-03-lww-trigger.sql
run_pg sql/postgres-05-sink-role.sql -v sink_user="$PG_SINK_USER" -v sink_pw="$PG_SINK_PASSWORD"

echo "Done."
