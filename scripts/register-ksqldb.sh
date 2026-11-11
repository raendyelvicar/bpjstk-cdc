#!/usr/bin/env bash
# Applies the ksqlDB loop filter (sql/ksqldb-01-loop-filter.sql).
# Run after the source connectors and before the sinks (register-connectors.sh does this).
set -e
KSQL=${KSQLDB_URL:-http://localhost:8088}
SQL_FILE=${SQL_FILE:-sql/ksqldb-01-loop-filter.sql}
# Partitions for a source topic that doesn't exist yet. Debezium only creates a
# table's topic on its first event, and ksqlDB can't create a stream on a missing
# topic. TODO(remote-kafka): match the remote cluster's num.partitions.
PARTITIONS=${KSQL_SOURCE_TOPIC_PARTITIONS:-1}
# Optional basic auth (e.g. a staging server or a Confluent Cloud API key/secret).
CURL_AUTH=()
[ -n "${KSQLDB_USER:-}" ] && CURL_AUTH=(-u "$KSQLDB_USER:${KSQLDB_PASSWORD:-}")

# POST statements to ksqlDB; fail on any statement error.
ksql() {
  local out
  out=$(jq -n --arg s "$1" '{ksql: $s, streamsProperties: {"auto.offset.reset": "earliest"}}' \
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

echo "Applying $SQL_FILE"
ksql "$(cat "$SQL_FILE")" | jq -r '.[] | "  " + (.commandStatus.message // ."@type")'
