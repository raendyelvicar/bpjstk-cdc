-- Login the bpjsk-mssql-sink connector writes with. Run after
-- mssql-03-lww-trigger.sql, as sa. sqlcmd variables (scripts/setup-db.sh passes them):
--   SINK_USER  login name (MSSQL_SINK_USER, default cdc_sink)
--   SINK_PW    its password (MSSQL_SINK_PASSWORD); must meet the password policy
--              and must not contain a single quote
-- Safe to re-run: the password is reset each time.
--
-- Loop prevention depends on this login: the LWW triggers stamp db_login =
-- ORIGINAL_LOGIN() on every row they write, and the ksqlDB loop filter drops
-- the SQL Server changes whose db_login is SINK_USER. Use it only for the sink.
--
-- Roles: db_datareader/db_datawriter for the upserts and deletes, db_ddladmin
-- because the sink runs SET IDENTITY_INSERT (needs ALTER on the table) and the
-- trigger runs DBCC CHECKIDENT ... RESEED.
USE master;
GO
IF SUSER_ID(N'$(SINK_USER)') IS NULL
    CREATE LOGIN [$(SINK_USER)] WITH PASSWORD = N'$(SINK_PW)', DEFAULT_DATABASE = [bpjsk_poc];
ELSE
    ALTER LOGIN [$(SINK_USER)] WITH PASSWORD = N'$(SINK_PW)';
GO
USE bpjsk_poc;
GO
IF USER_ID(N'$(SINK_USER)') IS NULL
    CREATE USER [$(SINK_USER)] FOR LOGIN [$(SINK_USER)];
ALTER ROLE db_datareader ADD MEMBER [$(SINK_USER)];
ALTER ROLE db_datawriter ADD MEMBER [$(SINK_USER)];
ALTER ROLE db_ddladmin   ADD MEMBER [$(SINK_USER)];
GO
