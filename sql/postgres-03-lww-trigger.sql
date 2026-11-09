-- Last-writer-wins: an update applies only if it beats the current row on
-- higher version, then newer last_updated_at, then higher last_updated_by.
-- Fires for application writes as well as replicated ones, including the DO
-- UPDATE branch of the sink's upsert. Keep the rule in sync with
-- mssql-03-lww-trigger.sql, or the nodes can pick different winners.
CREATE OR REPLACE FUNCTION public.orders_lww() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.version > OLD.version
       OR (NEW.version = OLD.version AND NEW.last_updated_at > OLD.last_updated_at)
       OR (NEW.version = OLD.version AND NEW.last_updated_at = OLD.last_updated_at
           AND NEW.last_updated_by COLLATE "C" > OLD.last_updated_by COLLATE "C")
    THEN
        RETURN NEW;
    END IF;
    RETURN NULL;  -- skip the update; the statement reports 0 rows for this row
END;
$$;

CREATE OR REPLACE TRIGGER orders_lww
    BEFORE UPDATE ON public.orders
    FOR EACH ROW EXECUTE FUNCTION public.orders_lww();
