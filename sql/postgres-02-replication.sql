-- Run as the owner of the bpjs_* tables (developer) on bpjsk_poc.
-- :pw is the password for the Debezium replication user. Leave it unset to
-- keep the role's current password (setup-db.sh does when the PostgreSQL source
-- connects as another login, PG_USER).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium_replication') THEN
    CREATE ROLE debezium_replication WITH LOGIN REPLICATION;
  END IF;
END $$;
\if :{?pw}
ALTER ROLE debezium_replication PASSWORD :'pw';
\endif

GRANT CONNECT ON DATABASE bpjsk_poc TO debezium_replication;
GRANT USAGE ON SCHEMA public TO debezium_replication;
GRANT SELECT ON public.bpjs_membership, public.bpjs_monthly_contribution, public.bpjs_claim
  TO debezium_replication;

-- The connector runs with publication.autocreate.mode=disabled, so the
-- publication must list exactly the tables in table.include.list.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication
      FOR TABLE public.bpjs_membership, public.bpjs_monthly_contribution, public.bpjs_claim;
  ELSE
    ALTER PUBLICATION dbz_publication
      SET TABLE public.bpjs_membership, public.bpjs_monthly_contribution, public.bpjs_claim;
  END IF;
END $$;

SELECT pg_create_logical_replication_slot('debezium_pg_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_pg_slot');

-- Key ranges: both nodes generate keys (SQL Server IDENTITY, PostgreSQL
-- sequences), and replicated upserts match on the key, so overlapping keys
-- would overwrite each other's rows. SQL Server keeps 1..999999999;
-- PostgreSQL starts at 1000000000. Only moves a sequence forward, so it is
-- safe to re-run.
SELECT setval(s.seq, 1000000000, false)
FROM (VALUES ('public.bpjs_membership_membership_id_seq'::regclass),
             ('public.bpjs_monthly_contribution_contribution_id_seq'::regclass),
             ('public.bpjs_claim_claim_id_seq'::regclass)) AS s(seq)
JOIN pg_sequences ps
  ON format('%I.%I', ps.schemaname, ps.sequencename)::regclass = s.seq
WHERE COALESCE(ps.last_value, 0) < 1000000000;
