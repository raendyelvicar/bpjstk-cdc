#!/usr/bin/env bash
# Applies the ksqlDB loop filter (sql/ksqldb-01-loop-filter.sql).
# Run after the source connectors and before the sinks (register-connectors.sh does this).
#   --replace  upgrade the running filter queries in place (CREATE OR REPLACE):
#              same query, same consumer offsets, so nothing is re-read. Use it
#              after changing a WHERE clause; ksqlDB rejects incompatible changes.
set -e
cd "$(dirname "$0")/.."
. scripts/env.sh
REPLACE=false
[ "${1:-}" = "--replace" ] && REPLACE=true
# The filter drops the changes the sink logins (PG_SINK_USER / MSSQL_SINK_USER,
# from scripts/env.sh) make; see the SQL file.
KSQL=$KSQLDB_URL
SQL_FILE=${SQL_FILE:-sql/ksqldb-01-loop-filter.sql}
# Partitions for a source topic that doesn't exist yet. Debezium only creates a
# table's topic on its first event, and ksqlDB can't create a stream on a missing
# topic. TODO(remote-kafka): match the remote cluster's num.partitions.
PARTITIONS=${KSQL_SOURCE_TOPIC_PARTITIONS:-1}
# Optional basic auth (e.g. a staging server or a Confluent Cloud API key/secret).
CURL_AUTH=()
[ -n "$KSQLDB_USER" ] && CURL_AUTH=(-u "$KSQLDB_USER:$KSQLDB_PASSWORD")

# POST statements to ksqlDB; fail on any statement error.
ksql() {
  local out
  out=$(jq -n --arg s "$1" --arg pg "$PG_SINK_USER" --arg ms "$MSSQL_SINK_USER" \
      '{ksql: $s, streamsProperties: {"auto.offset.reset": "earliest"},
        sessionVariables: {PG_SINK_USER: $pg, MSSQL_SINK_USER: $ms}}' \
    | curl -s "${CURL_AUTH[@]}" -X POST -H "Content-Type: application/vnd.ksql.v1+json" --data @- "$KSQL/ksql")
  if echo "$out" | jq -e 'type == "object" or any(.[]; ."@type" == "statement_error")' >/dev/null; then
    echo "$out" | jq . >&2
    return 1
  fi
  echo "$out"
}

echo "Waiting for ksqlDB at $KSQL"
for _ in $(seq 60); do
  curl -sf "${CURL_AUTH[@]}" "$KSQL/info" >/dev/null && break
  sleep 2
done
curl -sf "${CURL_AUTH[@]}" "$KSQL/info" >/dev/null || { echo "ksqlDB not reachable at $KSQL" >&2; exit 1; }

existing=$(ksql "LIST TOPICS;" | jq -r '.[0].topics[].name')
for t in $(grep -o "KAFKA_TOPIC='[^']*'" "$SQL_FILE" | grep -v "\.to-" | sed "s/KAFKA_TOPIC='//; s/'//"); do
  if ! grep -qxF "$t" <<<"$existing"; then
    echo "Creating missing source topic $t ($PARTITIONS partition(s))"
    # Creating and dropping a stream leaves the topic behind.
    ksql "CREATE STREAM tmp_create_topic (v STRING) WITH (KAFKA_TOPIC='$t', PARTITIONS=$PARTITIONS, VALUE_FORMAT='KAFKA');
          DROP STREAM tmp_create_topic;" >/dev/null
  fi
done

STATEMENTS=$(cat "$SQL_FILE")
if $REPLACE; then
  # Only the output (filter) streams; the raw source streams stay IF NOT EXISTS.
  STATEMENTS=$(sed -E 's/CREATE STREAM IF NOT EXISTS ([A-Za-z0-9_]+_to_(pg|mssql))([^A-Za-z0-9_]|$)/CREATE OR REPLACE STREAM \1\3/' <<<"$STATEMENTS")
fi

# Wait until ksqlDB has executed command $1. A request carrying
# commandSequenceNumber only runs once every command up to it is done; it gives
# up after a few seconds, so keep asking. (An in-place upgrade can take a minute.)
wait_for_command() {
  for _ in $(seq 150); do
    jq -n --argjson n "$1" '{ksql: "LIST STREAMS;", commandSequenceNumber: $n}' \
      | curl -s "${CURL_AUTH[@]}" -X POST -H "Content-Type: application/vnd.ksql.v1+json" --data @- "$KSQL/ksql" \
      | jq -e 'type == "array"' >/dev/null && return 0
    sleep 2
  done
  echo "ksqlDB did not finish command $1 within 5 minutes" >&2
  return 1
}

# One statement at a time, each confirmed before the next, so a slow upgrade
# can't make later statements time out and leave the filters half applied.
# Statements end with ';' at the end of a line; '--' comment lines are skipped.
echo "Applying $SQL_FILE (PG_SINK_USER=$PG_SINK_USER, MSSQL_SINK_USER=$MSSQL_SINK_USER)"
while IFS= read -r -d '' stmt; do
  out=$(ksql "$stmt")
  echo "$out" | jq -r '.[] | "  " + (.commandStatus.message // ."@type")'
  seq_no=$(echo "$out" | jq -r '[.[] | .commandSequenceNumber // empty] | max // empty')
  # No number when nothing was queued (e.g. IF NOT EXISTS on an existing stream).
  if [ -n "$seq_no" ] && [ "$seq_no" -ge 0 ]; then
    wait_for_command "$seq_no"
  fi
done < <(grep -v '^[[:space:]]*--' <<<"$STATEMENTS" \
           | awk '{ buf = buf $0 "\n" } /;[[:space:]]*$/ { printf "%s%c", buf, 0; buf = "" }')
