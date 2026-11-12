-- Disables the foreign keys on the replicated tables (public.bpjs_membership,
-- public.bpjs_monthly_contribution, public.bpjs_claim). Run as the table owner.
--
-- Why: each table replicates through its own Kafka topic, and the JDBC sink
-- buffers and flushes per table, so a child row (contribution, claim) can be
-- written before its parent membership, or a parent deleted before its
-- children. With the FK enforced, that fails the sink task. Without it, the
-- row is written and the order sorts itself out once the other table's change
-- arrives.
--
-- PostgreSQL can't switch a single FK off (NOT VALID still checks new rows),
-- so the FKs are dropped. Each definition is saved first in
-- public.bpjs_disabled_fk so the RESTORE section can recreate it exactly.
--
-- Trade-off: applies to every writer, so applications on this node can also
-- create orphan rows while the FKs are gone. Interim measure only (POC).
--
-- Not run by scripts/setup-db.sh. Safe to re-run (only drops FKs that still exist).

CREATE TABLE IF NOT EXISTS public.bpjs_disabled_fk (
    table_name text        NOT NULL,
    conname    text        NOT NULL,
    condef     text        NOT NULL,
    dropped_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, conname)
);

DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT c.conrelid::regclass::text AS tbl, c.conname, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        WHERE c.contype = 'f'
          AND c.conrelid IN ('public.bpjs_membership'::regclass,
                             'public.bpjs_monthly_contribution'::regclass,
                             'public.bpjs_claim'::regclass)
    LOOP
        INSERT INTO public.bpjs_disabled_fk (table_name, conname, condef)
        VALUES (r.tbl, r.conname, r.def)
        ON CONFLICT (table_name, conname) DO UPDATE SET condef = EXCLUDED.condef, dropped_at = now();
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.tbl, r.conname);
        RAISE NOTICE 'dropped % on %: %', r.conname, r.tbl, r.def;
    END LOOP;
END $$;

SELECT table_name, conname, condef, dropped_at FROM public.bpjs_disabled_fk ORDER BY table_name, conname;

-- ---------------------------------------------------------------- RESTORE
-- Recreates the saved FKs. NOT VALID enforces them for new rows without
-- checking existing ones; VALIDATE afterwards checks existing rows (fails if
-- orphans exist; find and fix them first).
--
-- DO $$
-- DECLARE r record;
-- BEGIN
--     FOR r IN SELECT * FROM public.bpjs_disabled_fk LOOP
--         EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s NOT VALID', r.table_name, r.conname, r.condef);
--         EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', r.table_name, r.conname);
--         DELETE FROM public.bpjs_disabled_fk WHERE table_name = r.table_name AND conname = r.conname;
--     END LOOP;
-- END $$;
