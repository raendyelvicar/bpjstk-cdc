USE bpjsk_poc;
GO
-- CDC capture and cleanup run as SQL Server Agent jobs. Without a running Agent
-- the change tables never fill and the Debezium source sees no changes.
IF NOT EXISTS (SELECT 1 FROM sys.dm_server_services
               WHERE servicename LIKE 'SQL Server Agent%' AND status_desc = 'Running')
    RAISERROR('WARNING: SQL Server Agent is not running; CDC will not capture changes until it is started.', 10, 1) WITH NOWAIT;
GO
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'bpjsk_poc' AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO
-- A capture instance keeps the column list it was created with. One created
-- before db_login existed would never report it, and the loop filter would let
-- every echo through, so recreate it ("offline" schema change: stop the
-- bpjsk-mssql-source connector first and start it again after this script).
DECLARE @t SYSNAME;
DECLARE stale CURSOR LOCAL FAST_FORWARD FOR
    SELECT OBJECT_NAME(ct.source_object_id)
    FROM cdc.change_tables ct
    WHERE ct.source_object_id IN (OBJECT_ID('dbo.BPJSMembership'), OBJECT_ID('dbo.BPJSMonthlyContribution'), OBJECT_ID('dbo.BPJSClaim'))
      AND NOT EXISTS (SELECT 1 FROM cdc.captured_columns cc
                      WHERE cc.object_id = ct.object_id AND cc.column_name = 'db_login');
OPEN stale;
FETCH NEXT FROM stale INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    RAISERROR('Recreating the CDC capture instance of dbo.%s to add db_login; restart bpjsk-mssql-source.', 10, 1, @t) WITH NOWAIT;
    EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = @t, @capture_instance = N'all';
    FETCH NEXT FROM stale INTO @t;
END;
CLOSE stale;
DEALLOCATE stale;
GO
-- Capture all columns. TotalAmount is computed, so CDC reports it as NULL; the
-- sinks drop it and each node computes its own.
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.BPJSMembership'))
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'BPJSMembership',
        @role_name            = NULL,
        @supports_net_changes = 1;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.BPJSMonthlyContribution'))
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'BPJSMonthlyContribution',
        @role_name            = NULL,
        @supports_net_changes = 1;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.BPJSClaim'))
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'BPJSClaim',
        @role_name            = NULL,
        @supports_net_changes = 1;
GO
EXEC sys.sp_cdc_change_job @job_type = N'capture', @maxtrans = 5000, @maxscans = 10, @continuous = 1, @pollinginterval = 1;
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 4320;
GO
