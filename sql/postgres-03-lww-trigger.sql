-- Last-writer-wins for public.bpjs_membership, public.bpjs_monthly_contribution,
-- public.bpjs_claim. An update applies only if it beats the current row on:
--   1. newer COALESCE(updated_at, created_at), then
--   2. higher COALESCE(updated_by, created_by)  (NULL loses).
-- An update that doesn't win, including an identical replicated echo, is
-- skipped, so it writes no WAL change and the replication loop stops here.
-- Fires for application writes as well as replicated ones, including the DO
-- UPDATE branch of the sink's upsert. Keep the rule in sync with
-- mssql-03-lww-trigger.sql, or the nodes can pick different winners.
--
-- Rules for writers (apps and people):
--   * Every UPDATE (soft deletes too) must set updated_at to a NEWER value and
--     set updated_by. An update that leaves updated_at unchanged is dropped.
--   * updated_by / created_by: SQL Server users < 1000000, PostgreSQL users
--     >= 1000000. The sinks' loop filter relies on this split.
--
-- All three tables share the same audit columns, so one function serves all.
CREATE OR REPLACE FUNCTION public.bpjs_lww() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    new_ts timestamptz := COALESCE(NEW.updated_at, NEW.created_at, '-infinity');
    old_ts timestamptz := COALESCE(OLD.updated_at, OLD.created_at, '-infinity');
BEGIN
    IF new_ts > old_ts
       OR (new_ts = old_ts
           AND COALESCE(NEW.updated_by, NEW.created_by, -1) > COALESCE(OLD.updated_by, OLD.created_by, -1))
    THEN
        RETURN NEW;
    END IF;
    RETURN NULL;  -- skip the update; the statement reports 0 rows for this row
END;
$$;

CREATE OR REPLACE TRIGGER bpjs_membership_lww
    BEFORE UPDATE ON public.bpjs_membership
    FOR EACH ROW EXECUTE FUNCTION public.bpjs_lww();

CREATE OR REPLACE TRIGGER bpjs_monthly_contribution_lww
    BEFORE UPDATE ON public.bpjs_monthly_contribution
    FOR EACH ROW EXECUTE FUNCTION public.bpjs_lww();

CREATE OR REPLACE TRIGGER bpjs_claim_lww
    BEFORE UPDATE ON public.bpjs_claim
    FOR EACH ROW EXECUTE FUNCTION public.bpjs_lww();
