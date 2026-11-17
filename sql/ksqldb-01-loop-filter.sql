-- Loop filter between the CDC topics and the JDBC sinks (replaces the
-- Debezium Filter SMT, which needs the debezium-scripting + Groovy plugin).
--
--   pg-bpjsk-cdc.public.<t>            -> <stream> -> pg-bpjsk-cdc.public.<t>.to-mssql         -> bpjsk-mssql-sink
--   mssql-bpjsk-cdc.bpjsk_poc.dbo.<T> -> <stream> -> mssql-bpjsk-cdc.bpjsk_poc.dbo.<T>.to-pg -> bpjsk-postgres-sink
--
-- Origin of a change = the database login that wrote it, after.db_login. Each
-- node's triggers stamp it on every insert and update (ORIGINAL_LOGIN() on SQL
-- Server, session_user on PostgreSQL), and each JDBC sink connects with its own
-- login, so a change whose db_login is the sink login of that node is a row the
-- sink just replicated there: it is dropped instead of being sent back. The
-- sink logins come in as ksqlDB variables from scripts/register-ksqldb.sh:
--   ${PG_SINK_USER}     the bpjsk-postgres-sink login (PG_SINK_USER, default cdc_sink)
--   ${MSSQL_SINK_USER}  the bpjsk-mssql-sink login (MSSQL_SINK_USER, default cdc_sink)
-- Compared case-insensitively. EXTRACTJSONFIELD returns the string 'null' for a
-- JSON null, hence the NULLIF.
-- Records with no db_login pass through: deletes (after = null; a replicated
-- delete finds no row on the origin and writes no change, so it stops there),
-- rows written before db_login existed, and non-JSON records (so the sink still
-- sends them to its DLQ). The LWW triggers stop any echo that gets through.
-- The CASE keeps EXTRACTJSONFIELD away from non-JSON values: it throws on
-- them, and an error in a WHERE clause drops the row (in a SELECT list the
-- same error only becomes NULL).
--
-- Key and value are read and written as raw strings (KAFKA format), so the
-- Debezium JSON envelope (schema + payload) reaches the sinks byte for byte,
-- with the same key and therefore the same partition. The source's own
-- tombstones are dropped here (a stream skips null values).
--
-- Deletes: each delete event (op = 'd') is replaced by a minimal delete
-- envelope with no columns, same key:
--   {"schema":{..."name":"bpjsk.delete.Envelope", before/after: empty structs, op},
--    "payload":{"before":{},"after":null,"op":"d"}}
-- The sinks' ExtractNewRecordState (delete.tombstone.handling.mode=tombstone)
-- flattens it to a null value with an empty schema, which the JDBC sink runs as
-- DELETE ... WHERE <key>. Why not the alternatives (checked on Debezium 2.5.4,
-- 2.6.2 and 3.6.3, which all behave the same here):
--   * a real tombstone (null value): ExtractNewRecordState in "tombstone" mode
--     DROPS tombstones, so the delete is lost silently - no error, no DLQ.
--   * the original delete envelope: it flattens to a null value whose schema
--     still has the snake_case columns (ReplaceField skips null values), so the
--     sink tries to ALTER the table, fails, and retries that record forever,
--     blocking the partition (errors.tolerance does not cover put()).
-- "before" must be non-null: with before and after both null the SMT treats
-- the record as a truncate and drops it.
--
-- Applied by scripts/register-ksqldb.sh. Every statement is IF NOT EXISTS, so
-- re-running is safe. To change a filter: scripts/register-ksqldb.sh --replace,
-- which runs the output streams as CREATE OR REPLACE, an in-place upgrade that
-- keeps each query's consumer offsets. Don't DROP and re-create them instead:
-- new queries re-read the CDC topics from the start, and with the foreign keys
-- on, a replayed child row can reach a sink before its parent, which fails the
-- sink task.

-- ---------------------------------------------------------------- PG -> MSSQL
CREATE STREAM IF NOT EXISTS pg_claim_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='pg-bpjsk-cdc.public.bpjs_claim', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS pg_claim_to_mssql
  WITH (KAFKA_TOPIC='pg-bpjsk-cdc.public.bpjs_claim.to-mssql', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, CASE WHEN v LIKE '{%' THEN CASE WHEN EXTRACTJSONFIELD(v, '$.payload.op') = 'd' THEN '{"schema":{"type":"struct","name":"bpjsk.delete.Envelope","optional":false,"fields":[{"type":"struct","optional":true,"field":"before","fields":[]},{"type":"struct","optional":true,"field":"after","fields":[]},{"type":"string","optional":false,"field":"op"}]},"payload":{"before":{},"after":null,"op":"d"}}' ELSE v END ELSE v END AS v
  FROM pg_claim_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(LCASE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.db_login'), 'null')), '')
                               <> LCASE('${PG_SINK_USER}') ELSE true END;

CREATE STREAM IF NOT EXISTS pg_membership_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='pg-bpjsk-cdc.public.bpjs_membership', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS pg_membership_to_mssql
  WITH (KAFKA_TOPIC='pg-bpjsk-cdc.public.bpjs_membership.to-mssql', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, CASE WHEN v LIKE '{%' THEN CASE WHEN EXTRACTJSONFIELD(v, '$.payload.op') = 'd' THEN '{"schema":{"type":"struct","name":"bpjsk.delete.Envelope","optional":false,"fields":[{"type":"struct","optional":true,"field":"before","fields":[]},{"type":"struct","optional":true,"field":"after","fields":[]},{"type":"string","optional":false,"field":"op"}]},"payload":{"before":{},"after":null,"op":"d"}}' ELSE v END ELSE v END AS v
  FROM pg_membership_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(LCASE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.db_login'), 'null')), '')
                               <> LCASE('${PG_SINK_USER}') ELSE true END;

CREATE STREAM IF NOT EXISTS pg_contribution_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='pg-bpjsk-cdc.public.bpjs_monthly_contribution', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS pg_contribution_to_mssql
  WITH (KAFKA_TOPIC='pg-bpjsk-cdc.public.bpjs_monthly_contribution.to-mssql', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, CASE WHEN v LIKE '{%' THEN CASE WHEN EXTRACTJSONFIELD(v, '$.payload.op') = 'd' THEN '{"schema":{"type":"struct","name":"bpjsk.delete.Envelope","optional":false,"fields":[{"type":"struct","optional":true,"field":"before","fields":[]},{"type":"struct","optional":true,"field":"after","fields":[]},{"type":"string","optional":false,"field":"op"}]},"payload":{"before":{},"after":null,"op":"d"}}' ELSE v END ELSE v END AS v
  FROM pg_contribution_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(LCASE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.db_login'), 'null')), '')
                               <> LCASE('${PG_SINK_USER}') ELSE true END;

-- ---------------------------------------------------------------- MSSQL -> PG
CREATE STREAM IF NOT EXISTS mssql_claim_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='mssql-bpjsk-cdc.bpjsk_poc.dbo.BPJSClaim', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS mssql_claim_to_pg
  WITH (KAFKA_TOPIC='mssql-bpjsk-cdc.bpjsk_poc.dbo.BPJSClaim.to-pg', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, CASE WHEN v LIKE '{%' THEN CASE WHEN EXTRACTJSONFIELD(v, '$.payload.op') = 'd' THEN '{"schema":{"type":"struct","name":"bpjsk.delete.Envelope","optional":false,"fields":[{"type":"struct","optional":true,"field":"before","fields":[]},{"type":"struct","optional":true,"field":"after","fields":[]},{"type":"string","optional":false,"field":"op"}]},"payload":{"before":{},"after":null,"op":"d"}}' ELSE v END ELSE v END AS v
  FROM mssql_claim_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(LCASE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.db_login'), 'null')), '')
                               <> LCASE('${MSSQL_SINK_USER}') ELSE true END;

CREATE STREAM IF NOT EXISTS mssql_membership_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='mssql-bpjsk-cdc.bpjsk_poc.dbo.BPJSMembership', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS mssql_membership_to_pg
  WITH (KAFKA_TOPIC='mssql-bpjsk-cdc.bpjsk_poc.dbo.BPJSMembership.to-pg', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, CASE WHEN v LIKE '{%' THEN CASE WHEN EXTRACTJSONFIELD(v, '$.payload.op') = 'd' THEN '{"schema":{"type":"struct","name":"bpjsk.delete.Envelope","optional":false,"fields":[{"type":"struct","optional":true,"field":"before","fields":[]},{"type":"struct","optional":true,"field":"after","fields":[]},{"type":"string","optional":false,"field":"op"}]},"payload":{"before":{},"after":null,"op":"d"}}' ELSE v END ELSE v END AS v
  FROM mssql_membership_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(LCASE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.db_login'), 'null')), '')
                               <> LCASE('${MSSQL_SINK_USER}') ELSE true END;

CREATE STREAM IF NOT EXISTS mssql_contribution_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='mssql-bpjsk-cdc.bpjsk_poc.dbo.BPJSMonthlyContribution', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS mssql_contribution_to_pg
  WITH (KAFKA_TOPIC='mssql-bpjsk-cdc.bpjsk_poc.dbo.BPJSMonthlyContribution.to-pg', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, CASE WHEN v LIKE '{%' THEN CASE WHEN EXTRACTJSONFIELD(v, '$.payload.op') = 'd' THEN '{"schema":{"type":"struct","name":"bpjsk.delete.Envelope","optional":false,"fields":[{"type":"struct","optional":true,"field":"before","fields":[]},{"type":"struct","optional":true,"field":"after","fields":[]},{"type":"string","optional":false,"field":"op"}]},"payload":{"before":{},"after":null,"op":"d"}}' ELSE v END ELSE v END AS v
  FROM mssql_contribution_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(LCASE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.db_login'), 'null')), '')
                               <> LCASE('${MSSQL_SINK_USER}') ELSE true END;
