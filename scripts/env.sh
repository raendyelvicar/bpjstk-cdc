# Connection settings for every script, connector config and test.
# Source it (". scripts/env.sh"); it reads .env.$APP_ENV (or $ENV_FILE), fills
# in the defaults below and exports everything, so the connector configs'
# ${env:VAR} placeholders can be filled in by scripts/render-config.sh.
#
#   APP_ENV=local        .env.local        (default) local docker-compose stack
#   APP_ENV=development  .env.development  *.kafka.kodehive.id
#   APP_ENV=staging      .env.staging
#
# Defaults below are for the local docker-compose stack (see .env.example).

APP_ENV=${APP_ENV:-local}
ENV_FILE=${ENV_FILE:-.env.$APP_ENV}
if [ ! -f "$ENV_FILE" ]; then
  echo "env.sh: $ENV_FILE not found (APP_ENV=$APP_ENV); copy .env.example to it." >&2
  return 1 2>/dev/null || exit 1
fi
# Announce once: child scripts (render-config.sh) source this again.
[ -n "${ENV_SH_ANNOUNCED:-}" ] || { echo "env.sh: using $ENV_FILE" >&2; export ENV_SH_ANNOUNCED=1; }
# Read VAR=value lines literally (no quote removal, no $ expansion, so
# passwords with $ survive). A variable already set in the environment wins,
# e.g. "PG_HOST=localhost scripts/setup-db.sh".
if [ -f "$ENV_FILE" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    [[ $line =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || continue
    key=${BASH_REMATCH[1]}
    [ -n "${!key+x}" ] || export "$key=${BASH_REMATCH[2]%$'\r'}"
  done < "$ENV_FILE"
  unset line key
fi

: "${MSSQL_PASSWORD:?set MSSQL_PASSWORD in $ENV_FILE}"
: "${PG_PASSWORD:?set PG_PASSWORD in $ENV_FILE}"

# ---- Databases as reached from the Kafka Connect worker. Read before the
# "from this machine" defaults below: when .env names a host, the worker uses
# it too; otherwise it's the docker-compose service name.
CONNECT_MSSQL_HOST=${CONNECT_MSSQL_HOST:-${MSSQL_HOST:-sqlserver}}
CONNECT_MSSQL_PORT=${CONNECT_MSSQL_PORT:-${MSSQL_HOST:+${MSSQL_PORT:-1433}}}
CONNECT_MSSQL_PORT=${CONNECT_MSSQL_PORT:-1433}
CONNECT_PG_HOST=${CONNECT_PG_HOST:-${PG_HOST:-postgres}}
CONNECT_PG_PORT=${CONNECT_PG_PORT:-${PG_HOST:+${PG_PORT:-5432}}}
CONNECT_PG_PORT=${CONNECT_PG_PORT:-5432}
# Broker for the SQL Server source's schema-history client (worker's view).
CONNECT_KAFKA_BOOTSTRAP=${CONNECT_KAFKA_BOOTSTRAP:-kafka:29092}

# ---- Databases as reached from this machine (setup-db.sh, tests).
MSSQL_HOST=${MSSQL_HOST:-localhost}
MSSQL_PORT=${MSSQL_PORT:-1433}
MSSQL_DB=${MSSQL_DB:-bpjsk_poc}
PG_HOST=${PG_HOST:-localhost}
PG_PORT=${PG_PORT:-5432}
PG_DB=${PG_DB:-bpjsk_poc}

# ---- Logins
# Admins: setup-db.sh and the tests. MSSQL_PASSWORD / PG_PASSWORD are theirs.
MSSQL_ADMIN_USER=${MSSQL_ADMIN_USER:-sa}
PG_ADMIN_USER=${PG_ADMIN_USER:-postgres}
# Debezium sources (read only). The PostgreSQL one needs REPLICATION.
MSSQL_USER=${MSSQL_USER:-$MSSQL_ADMIN_USER}
MSSQL_USER_PASSWORD=${MSSQL_USER_PASSWORD:-$MSSQL_PASSWORD}
PG_USER=${PG_USER:-debezium_replication}
PG_USER_PASSWORD=${PG_USER_PASSWORD:-$PG_PASSWORD}
# JDBC sinks. The loop filter drops every change these logins write, so they
# must be used by the sinks only (setup-db.sh creates them).
MSSQL_SINK_USER=${MSSQL_SINK_USER:-cdc_sink}
MSSQL_SINK_PASSWORD=${MSSQL_SINK_PASSWORD:-$MSSQL_PASSWORD}
PG_SINK_USER=${PG_SINK_USER:-cdc_sink}
PG_SINK_PASSWORD=${PG_SINK_PASSWORD:-$PG_PASSWORD}
# Where the sinks write; defaults to the same databases as the sources.
MSSQL_SINK_HOST=${MSSQL_SINK_HOST:-$CONNECT_MSSQL_HOST}
MSSQL_SINK_PORT=${MSSQL_SINK_PORT:-$CONNECT_MSSQL_PORT}
MSSQL_SINK_DB=${MSSQL_SINK_DB:-$MSSQL_DB}
PG_SINK_HOST=${PG_SINK_HOST:-$CONNECT_PG_HOST}
PG_SINK_PORT=${PG_SINK_PORT:-$CONNECT_PG_PORT}
PG_SINK_DB=${PG_SINK_DB:-$PG_DB}

# ---- Endpoints (user/password optional, for basic auth)
CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
CONNECT_USER=${CONNECT_USER:-}
CONNECT_PWD=${CONNECT_PWD:-}
KSQLDB_URL=${KSQLDB_URL:-http://localhost:8088}
KSQLDB_USER=${KSQLDB_USER:-}
KSQLDB_PASSWORD=${KSQLDB_PASSWORD:-}
# inline: render-config.sh writes the passwords into the connector configs
# (anyone with access to the Connect REST API can read them). worker: they stay
# ${env:...} and the Connect worker must have them set (the local compose does).
CONNECT_SECRETS=${CONNECT_SECRETS:-inline}

export CONNECT_MSSQL_HOST CONNECT_MSSQL_PORT CONNECT_PG_HOST CONNECT_PG_PORT CONNECT_KAFKA_BOOTSTRAP \
  MSSQL_HOST MSSQL_PORT MSSQL_DB PG_HOST PG_PORT PG_DB MSSQL_ADMIN_USER PG_ADMIN_USER \
  MSSQL_PASSWORD PG_PASSWORD MSSQL_USER MSSQL_USER_PASSWORD PG_USER PG_USER_PASSWORD \
  MSSQL_SINK_USER MSSQL_SINK_PASSWORD PG_SINK_USER PG_SINK_PASSWORD \
  MSSQL_SINK_HOST MSSQL_SINK_PORT MSSQL_SINK_DB PG_SINK_HOST PG_SINK_PORT PG_SINK_DB \
  CONNECT_URL CONNECT_USER CONNECT_PWD KSQLDB_URL KSQLDB_USER KSQLDB_PASSWORD CONNECT_SECRETS

# A sink login shared with a writer breaks replication silently: the loop
# filter drops that writer's changes as echoes. Refuse the obvious cases.
check_sink_logins() {
  local lc_ms lc_pg
  lc_ms=$(tr '[:upper:]' '[:lower:]' <<<"$MSSQL_SINK_USER")
  lc_pg=$(tr '[:upper:]' '[:lower:]' <<<"$PG_SINK_USER")
  if [ "$lc_ms" = "$(tr '[:upper:]' '[:lower:]' <<<"$MSSQL_ADMIN_USER")" ]; then
    echo "MSSQL_SINK_USER ($MSSQL_SINK_USER) must not be the admin login: the loop filter would drop its writes." >&2
    return 1
  fi
  for u in "$PG_ADMIN_USER" "$PG_USER"; do
    if [ "$lc_pg" = "$(tr '[:upper:]' '[:lower:]' <<<"$u")" ]; then
      echo "PG_SINK_USER ($PG_SINK_USER) must not be $u (admin/source login): the loop filter would drop its writes." >&2
      return 1
    fi
  done
}
