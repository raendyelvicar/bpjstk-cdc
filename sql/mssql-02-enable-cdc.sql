USE ProductionDB;
GO
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ProductionDB' AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Orders'))
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'Orders',
        @role_name            = NULL,
        @supports_net_changes = 1,
        @captured_column_list = N'OrderID, CustomerID, OrderStatus, OrderTotal, last_updated_at, last_updated_by, version';
GO
EXEC sys.sp_cdc_change_job @job_type = N'capture', @maxtrans = 5000, @maxscans = 10, @continuous = 1, @pollinginterval = 1;
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 4320;
GO