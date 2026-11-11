#!/usr/bin/env bash
set -e
# Kafka Connect REST endpoint (the Connect worker, not the broker). Change only
# if Connect itself also moves off this machine.
URL=${CONNECT_URL:-http://localhost:8083}/connectors

register() {
  for f in "$@"; do
    echo "Registering $f"
    # Drop the "TODO(...)" and "NOTE ..." annotation keys before sending the config to Connect.
    jq '.config |= with_entries(select(.key | (startswith("TODO(") or startswith("NOTE ")) | not))' "$f" \
      | curl -s -X POST -H "Content-Type: application/json" --data @- "$URL"
    echo
  done
}

# Order matters: sources create the CDC topics, ksqlDB filters them into the
# *.to-mssql / *.to-pg topics (loop prevention), and the sinks read those.
register configs/source/bpjstk-mssql-source.json \
         configs/source/bpjstk-postgres-source.json
"$(dirname "$0")/register-ksqldb.sh"
register configs/sink/bpjstk-postgres-sink.json \
         configs/sink/bpjstk-mssql-sink.json
