#!/usr/bin/env bash
# Runs folders of the "BPJS CDC Ops" Postman collection against APP_ENV, with
# every value (hosts, logins, passwords) taken from .env.$APP_ENV through
# scripts/env.sh, i.e. exactly what the shell scripts use.
#
#   bash scripts/postman-run.sh "1 Check"                          # local
#   APP_ENV=development bash scripts/postman-run.sh "1 Check"
#   APP_ENV=development bash scripts/postman-run.sh "2 Pause" "3 Update connectors"
#
# Folders: "1 Check" (read only), "2 Pause", "3 Update connectors",
# "4 Loop filter", "5 Resume", "6 Restart failed tasks". At least one is
# required, so a bare run can't pause and reconfigure everything by accident.
# Extra Postman CLI flags go after "--": ... "1 Check" -- --verbose
#
# Passwords are passed with --env-var, so they are visible to other users of
# this machine in the process list while the run lasts. Regenerate the
# collection after changing configs/ or the ksqlDB SQL: python3 scripts/gen-postman.py
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/env.sh
check_sink_logins

items=() extra=()
while [ $# -gt 0 ]; do
  case $1 in
    --) shift; extra=("$@"); break ;;
    *) items+=(-i "$1"); shift ;;
  esac
done
[ ${#items[@]} -gt 0 ] || { sed -n '2,13p' "$0" >&2; exit 2; }

vars=(CONNECT_URL CONNECT_USER CONNECT_PWD KSQLDB_URL KSQLDB_USER KSQLDB_PASSWORD
      CONNECT_MSSQL_HOST CONNECT_MSSQL_PORT CONNECT_PG_HOST CONNECT_PG_PORT CONNECT_KAFKA_BOOTSTRAP
      MSSQL_DB PG_DB MSSQL_USER MSSQL_USER_PASSWORD PG_USER PG_USER_PASSWORD
      MSSQL_SINK_USER MSSQL_SINK_PASSWORD MSSQL_SINK_HOST MSSQL_SINK_PORT MSSQL_SINK_DB
      PG_SINK_USER PG_SINK_PASSWORD PG_SINK_HOST PG_SINK_PORT PG_SINK_DB)
args=()
for v in "${vars[@]}"; do
  val=${!v}
  # CONNECT_SECRETS=worker: leave passwords for the Connect worker to resolve.
  if [ "$CONNECT_SECRETS" = worker ] && [[ $v == *_PASSWORD ]] && [[ $v != KSQLDB_* ]]; then
    val="\${env:$v}"
  fi
  args+=(--env-var "$v=$val")
done

env_file="postman/environments/$APP_ENV.environment.yaml"
[ -f "$env_file" ] && args+=(-e "$env_file")

echo "postman-run: APP_ENV=$APP_ENV, Connect $CONNECT_URL, ksqlDB $KSQLDB_URL" >&2
exec postman collection run "postman/collections/bpjsk-cdc-ops" "${args[@]}" "${items[@]}" ${extra[@]+"${extra[@]}"}
