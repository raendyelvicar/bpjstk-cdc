USE bpjsk_poc;
GO
-- Disables the foreign keys on the replicated tables (dbo.BPJSMembership,
-- dbo.BPJSMonthlyContribution, dbo.BPJSClaim).
--
-- Why: each table replicates through its own Kafka topic, and the JDBC sink
-- buffers and flushes per table, so a child row (contribution, claim) can be
-- written before its parent membership, or a parent deleted before its
-- children. With the FK enforced, that fails the sink task
-- ("The INSERT statement conflicted with the FOREIGN KEY constraint").
-- Disabled, the row is written and the order sorts itself out once the
-- other table's change arrives.
--
-- Trade-off: NOCHECK applies to every writer, so applications on this node
-- can also create orphan rows while the FKs are disabled. Interim measure only
-- (POC); see the RESTORE section below.
--
-- Not run by scripts/setup-db.sh. Safe to re-run (skips FKs already disabled).

DECLARE @sql NVARCHAR(MAX) = N'';
SELECT @sql += N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
             + N' NOCHECK CONSTRAINT ' + QUOTENAME(fk.name) + N';' + CHAR(10)
FROM sys.foreign_keys fk
JOIN sys.tables t ON t.object_id = fk.parent_object_id
WHERE fk.parent_object_id IN (OBJECT_ID('dbo.BPJSMembership'), OBJECT_ID('dbo.BPJSMonthlyContribution'), OBJECT_ID('dbo.BPJSClaim'))
  AND fk.is_disabled = 0;

IF @sql = N''
    PRINT 'No enabled foreign keys on the replicated tables.';
ELSE
BEGIN
    PRINT @sql;
    EXEC sys.sp_executesql @sql;
END;
GO

SELECT OBJECT_NAME(fk.parent_object_id) AS table_name, fk.name AS fk_name,
       OBJECT_NAME(fk.referenced_object_id) AS references_table, fk.is_disabled, fk.is_not_trusted
FROM sys.foreign_keys fk
WHERE fk.parent_object_id IN (OBJECT_ID('dbo.BPJSMembership'), OBJECT_ID('dbo.BPJSMonthlyContribution'), OBJECT_ID('dbo.BPJSClaim'));
GO

-- ---------------------------------------------------------------- RESTORE
-- Re-enable and re-check existing rows (fails if orphans exist; find and fix
-- them first). WITH CHECK makes the constraint trusted again.
--
-- DECLARE @sql NVARCHAR(MAX) = N'';
-- SELECT @sql += N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
--              + N' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(fk.name) + N';' + CHAR(10)
-- FROM sys.foreign_keys fk JOIN sys.tables t ON t.object_id = fk.parent_object_id
-- WHERE fk.parent_object_id IN (OBJECT_ID('dbo.BPJSMembership'), OBJECT_ID('dbo.BPJSMonthlyContribution'), OBJECT_ID('dbo.BPJSClaim'))
--   AND fk.is_disabled = 1;
-- PRINT @sql; EXEC sys.sp_executesql @sql;
