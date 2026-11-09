DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium_replication') THEN
    CREATE ROLE debezium_replication WITH LOGIN REPLICATION;
  END IF;
END $$;
ALTER ROLE debezium_replication PASSWORD :'pw';

GRANT CONNECT ON DATABASE "ProductionDB" TO debezium_replication;
GRANT USAGE ON SCHEMA public TO debezium_replication;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_replication;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR TABLE public.orders;
  END IF;
END $$;

SELECT pg_create_logical_replication_slot('debezium_pg_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_pg_slot');