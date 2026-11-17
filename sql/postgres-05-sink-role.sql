-- Role the bpjsk-postgres-sink connector writes with. Run after
-- postgres-03-lww-trigger.sql, as the owner of the bpjs_* tables. psql variables
-- (scripts/setup-db.sh passes them):
--   sink_user  role name (PG_SINK_USER, default cdc_sink)
--   sink_pw    its password (PG_SINK_PASSWORD)
-- Safe to re-run: the password is reset each time.
--
-- Loop prevention depends on this role: the db_login trigger stamps
-- session_user on every row written, and the ksqlDB loop filter drops the
-- PostgreSQL changes whose db_login is sink_user. Use it only for the sink.
SELECT format('CREATE ROLE %I WITH LOGIN', :'sink_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'sink_user') \gexec
ALTER ROLE :"sink_user" PASSWORD :'sink_pw';

GRANT CONNECT ON DATABASE bpjsk_poc TO :"sink_user";
GRANT USAGE ON SCHEMA public TO :"sink_user";
GRANT SELECT, INSERT, UPDATE, DELETE
  ON public.bpjs_membership, public.bpjs_monthly_contribution, public.bpjs_claim
  TO :"sink_user";
