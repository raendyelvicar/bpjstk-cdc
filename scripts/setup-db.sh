#!/usr/bin/env bash
set -e
set -a; source .env; set +a

# Pick whichever sqlcmd exists in the SQL Server image
SQLCMD=$(docker exec sqlserver sh -c 'ls /opt/mssql-tools18/bin/sqlcmd 2>/dev/null || ls /opt/mssql-tools/bin/sqlcmd')

run_mssql() {
  docker exec -i sqlserver "$SQLCMD" -S localhost -U sa -P "$MSSQL_PASSWORD" -C -b -i /dev/stdin < "$1"
}

echo "== SQL Server =="
run_mssql sql/mssql-02-enable-cdc.sql
run_mssql sql/mssql-03-lww-trigger.sql

echo "== PostgreSQL =="
docker exec -i postgres psql -U postgres -d ProductionDB -v ON_ERROR_STOP=1 -v pw="$PG_PASSWORD" \
  < sql/postgres-02-replication.sql
docker exec -i postgres psql -U postgres -d ProductionDB -v ON_ERROR_STOP=1 \
  < sql/postgres-03-lww-trigger.sql

echo "Done."