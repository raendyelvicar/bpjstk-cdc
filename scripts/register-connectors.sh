#!/usr/bin/env bash
# Creates or updates the connectors on the Kafka Connect worker at CONNECT_URL,
# with their configs filled in from .env.$APP_ENV (scripts/render-config.sh).
#   scripts/register-connectors.sh                 # sources, ksqlDB filter, sinks
#   scripts/register-connectors.sh configs/sink/*  # just these connectors
#   scripts/register-connectors.sh --dry-run ...   # print the configs (passwords masked), send nothing
# Uses PUT /connectors/<name>/config: creates a missing connector, updates an
# existing one in place (offsets kept; a paused connector stays paused).
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/env.sh
check_sink_logins

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && { DRY_RUN=true; shift; }
CURL_AUTH=()
[ -n "$CONNECT_USER" ] && CURL_AUTH=(-u "$CONNECT_USER:$CONNECT_PWD")

put() {
  for f in "$@"; do
    name=$(jq -r .name "$f")
    if $DRY_RUN; then
      echo "== $name -> $CONNECT_URL (dry run)"
      bash scripts/render-config.sh --mask "$f"
      continue
    fi
    echo -n "Registering $name ($f) -> $CONNECT_URL: "
    out=$(bash scripts/render-config.sh "$f" \
      | curl -sS ${CURL_AUTH[@]+"${CURL_AUTH[@]}"} -X PUT -H "Content-Type: application/json" --data @- \
             -w '\n%{http_code}' "$CONNECT_URL/connectors/$name/config")
    code=$(tail -n1 <<<"$out")
    case $code in
      200) echo "updated" ;;
      201) echo "created" ;;
      401|403) echo "HTTP $code"
        echo "Connect at $CONNECT_URL rejected the credentials: set CONNECT_USER / CONNECT_PWD in $ENV_FILE." >&2
        exit 1 ;;
      *) echo "HTTP $code"; sed '$d' <<<"$out" | jq . 2>/dev/null || sed '$d' <<<"$out"; exit 1 ;;
    esac
  done
}

if [ $# -gt 0 ]; then
  put "$@"
  exit 0
fi

# Order matters: sources create the CDC topics, ksqlDB filters them into the
# *.to-mssql / *.to-pg topics (loop prevention), and the sinks read those.
put configs/source/bpjsk-mssql-source.json configs/source/bpjsk-postgres-source.json
$DRY_RUN || bash scripts/register-ksqldb.sh
put configs/sink/bpjsk-postgres-sink.json configs/sink/bpjsk-mssql-sink.json
