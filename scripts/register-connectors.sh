#!/usr/bin/env bash
set -e
URL=http://localhost:8083/connectors
for f in configs/source/mssql-source-orders.json \
         configs/source/postgres-source-orders.json \
         configs/sink/postgres-sink-orders.json \
         configs/sink/mssql-sink-orders.json; do
  echo "Registering $f"
  curl -s -X POST -H "Content-Type: application/json" --data @"$f" $URL
  echo
done