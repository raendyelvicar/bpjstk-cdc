#!/usr/bin/env bash
# Prints a connector's config (the "config" object, ready for
# PUT /connectors/<name>/config) with its ${env:VAR} placeholders filled in
# from .env.$APP_ENV via scripts/env.sh.
#   scripts/render-config.sh configs/sink/bpjsk-mssql-sink.json
#   scripts/render-config.sh --mask configs/...   # passwords shown as ***
# CONNECT_SECRETS=worker keeps the *PASSWORD* placeholders as ${env:...} for
# the Connect worker to resolve. "NOTE ..." / "TODO(...)" keys are dropped.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/env.sh

MASK=false
[ "${1:-}" = "--mask" ] && { MASK=true; shift; }
f=${1:?usage: render-config.sh [--mask] <connector.json>}

jq --argjson mask "$MASK" --arg secrets "$CONNECT_SECRETS" '
  .config
  | with_entries(select(.key | (startswith("TODO(") or startswith("NOTE ")) | not))
  | with_entries(.value |= (
      if type == "string" then
        gsub("\\$\\{env:(?<v>[A-Za-z0-9_]+)\\}";
             if (.v | test("PASSWORD")) and $secrets == "worker" then "${env:" + .v + "}"
             elif (.v | test("PASSWORD")) and $mask then "***"
             elif $ENV[.v] == null or $ENV[.v] == "" then error("\(.v) is not set (see .env.example)")
             else $ENV[.v] end)
      else . end))
' "$f"
