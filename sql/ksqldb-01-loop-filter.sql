-- Loop filter between the CDC topics and the JDBC sinks (replaces the
-- Debezium Filter SMT, which needs the debezium-scripting + Groovy plugin).
--
--   pg-bpjstk-cdc.public.<t>            -> <stream> -> pg-bpjstk-cdc.public.<t>.to-mssql         -> bpjstk-mssql-sink
--   mssql-bpjstk-cdc.BPJSTK_POC.dbo.<T> -> <stream> -> mssql-bpjstk-cdc.BPJSTK_POC.dbo.<T>.to-pg -> bpjstk-postgres-sink
--
-- Origin of a change = after.updated_by, else after.created_by.
-- EXTRACTJSONFIELD returns the string 'null' for a JSON null (Debezium sends
-- every column, so an insert has "updated_by": null), hence the NULLIF.
-- SQL Server users are < 1000000, PostgreSQL users >= 1000000.
-- Each output keeps only the changes that originated on the source side, so a
-- row a sink just wrote is not sent back to where it came from.
-- Records with no origin pass through: deletes (after = null), rows with
-- neither column set, and non-JSON records (so the sink still sends them to
-- its DLQ). The LWW triggers stop any echo that gets through.
-- The CASE keeps EXTRACTJSONFIELD away from non-JSON values: it throws on
-- them, and an error in a WHERE clause drops the row (in a SELECT list the
-- same error only becomes NULL).
--
-- Key and value are read and written as raw strings (KAFKA format), so the
-- Debezium JSON envelope (schema + payload) reaches the sinks byte for byte,
-- with the same key and therefore the same partition. Tombstones are dropped
-- here; the sinks rebuild them from the delete events (delete-to-tombstone).
--
-- Applied by scripts/register-ksqldb.sh. Every statement is IF NOT EXISTS, so
-- re-running is safe. To change a filter: TERMINATE the query, DROP STREAM the
-- output stream (not the topic), then re-run.

-- ---------------------------------------------------------------- PG -> MSSQL
CREATE STREAM IF NOT EXISTS pg_claim_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='pg-bpjstk-cdc.public.bpjs_claim', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS pg_claim_to_mssql
  WITH (KAFKA_TOPIC='pg-bpjstk-cdc.public.bpjs_claim.to-mssql', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, v FROM pg_claim_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(CAST(COALESCE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.updated_by'), 'null'),
                               NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.created_by'), 'null')) AS BIGINT), CAST(1000000 AS BIGINT)) >= 1000000 ELSE true END;

CREATE STREAM IF NOT EXISTS pg_membership_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='pg-bpjstk-cdc.public.bpjs_membership', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS pg_membership_to_mssql
  WITH (KAFKA_TOPIC='pg-bpjstk-cdc.public.bpjs_membership.to-mssql', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, v FROM pg_membership_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(CAST(COALESCE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.updated_by'), 'null'),
                               NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.created_by'), 'null')) AS BIGINT), CAST(1000000 AS BIGINT)) >= 1000000 ELSE true END;

CREATE STREAM IF NOT EXISTS pg_contribution_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='pg-bpjstk-cdc.public.bpjs_monthly_contribution', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS pg_contribution_to_mssql
  WITH (KAFKA_TOPIC='pg-bpjstk-cdc.public.bpjs_monthly_contribution.to-mssql', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, v FROM pg_contribution_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(CAST(COALESCE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.updated_by'), 'null'),
                               NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.created_by'), 'null')) AS BIGINT), CAST(1000000 AS BIGINT)) >= 1000000 ELSE true END;

-- ---------------------------------------------------------------- MSSQL -> PG
CREATE STREAM IF NOT EXISTS mssql_claim_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='mssql-bpjstk-cdc.BPJSTK_POC.dbo.BPJSClaim', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS mssql_claim_to_pg
  WITH (KAFKA_TOPIC='mssql-bpjstk-cdc.BPJSTK_POC.dbo.BPJSClaim.to-pg', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, v FROM mssql_claim_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(CAST(COALESCE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.updated_by'), 'null'),
                               NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.created_by'), 'null')) AS BIGINT), CAST(0 AS BIGINT)) < 1000000 ELSE true END;

CREATE STREAM IF NOT EXISTS mssql_membership_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='mssql-bpjstk-cdc.BPJSTK_POC.dbo.BPJSMembership', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS mssql_membership_to_pg
  WITH (KAFKA_TOPIC='mssql-bpjstk-cdc.BPJSTK_POC.dbo.BPJSMembership.to-pg', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, v FROM mssql_membership_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(CAST(COALESCE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.updated_by'), 'null'),
                               NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.created_by'), 'null')) AS BIGINT), CAST(0 AS BIGINT)) < 1000000 ELSE true END;

CREATE STREAM IF NOT EXISTS mssql_contribution_raw (k STRING KEY, v STRING)
  WITH (KAFKA_TOPIC='mssql-bpjstk-cdc.BPJSTK_POC.dbo.BPJSMonthlyContribution', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA');
CREATE STREAM IF NOT EXISTS mssql_contribution_to_pg
  WITH (KAFKA_TOPIC='mssql-bpjstk-cdc.BPJSTK_POC.dbo.BPJSMonthlyContribution.to-pg', KEY_FORMAT='KAFKA', VALUE_FORMAT='KAFKA') AS
  SELECT k, v FROM mssql_contribution_raw
  WHERE CASE WHEN v LIKE '{%' THEN COALESCE(CAST(COALESCE(NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.updated_by'), 'null'),
                               NULLIF(EXTRACTJSONFIELD(v, '$.payload.after.created_by'), 'null')) AS BIGINT), CAST(0 AS BIGINT)) < 1000000 ELSE true END;
