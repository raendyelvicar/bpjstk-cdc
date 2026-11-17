USE bpjsk_poc;
GO
-- Triggers keep the QUOTED_IDENTIFIER/ANSI_NULLS in effect when created, and
-- inserting into BPJSMonthlyContribution (persisted computed TotalAmount)
-- needs both ON. sqlcmd defaults QUOTED_IDENTIFIER to OFF.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
-- Last-writer-wins for dbo.BPJSMembership, dbo.BPJSMonthlyContribution, dbo.BPJSClaim.
-- An update applies only if it beats the current row on:
--   1. newer COALESCE(updated_at, CreatedAt), then
--   2. higher COALESCE(updated_by, created_by)  (NULL loses).
-- An update that doesn't win, including an identical replicated echo, is
-- skipped, so it writes no CDC row and the replication loop stops here.
-- Keep the rule in sync with postgres-03-lww-trigger.sql, or the nodes can
-- pick different winners.
--
-- Rules for writers (apps and people):
--   * Every UPDATE (soft deletes too) must set updated_at to a NEWER value and
--     set updated_by. An update that leaves updated_at unchanged is dropped.
--   * Write timestamps in UTC (SYSUTCDATETIME()). PostgreSQL stores
--     timestamptz and replicated values are treated as UTC. Note that the
--     CreatedAt default is GETDATE() (server local time).
--   * Don't connect as the sink login (MSSQL_SINK_USER, default cdc_sink):
--     the loop filter treats every row it writes as a replicated echo and
--     never sends it to PostgreSQL.
--
-- db_login: every row this trigger writes gets db_login = ORIGINAL_LOGIN(),
-- the login of the connection (whatever the writer put in the column is
-- ignored). CDC records it, and the ksqlDB loop filter drops the changes whose
-- db_login is the sink's login, i.e. the ones the sink just replicated here.
-- Covers INSERT too because the sink's MERGE has both branches, and SQL Server
-- requires INSTEAD OF triggers for every action in a MERGE if any has one.
-- TotalAmount is computed, so it is never written.
--
-- Identity keys: app inserts leave the key to IDENTITY (the inserted table
-- shows 0/NULL for it); replicated inserts from the sink carry the PostgreSQL
-- key and are inserted with IDENTITY_INSERT ON, after which the trigger
-- reseeds IDENTITY back so it stays below PostgreSQL's range (>= 1000000000).
--
-- Timestamps compare at DATETIME2(6) to match PostgreSQL's microseconds. Events
-- from SQL Server still arrive with millisecond precision
-- (time.precision.mode=connect), so ties within one millisecond can resolve
-- differently on each node.
CREATE OR ALTER TRIGGER dbo.tr_BPJSMembership_LWW ON dbo.BPJSMembership
INSTEAD OF INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- This trigger joins inserted to deleted on the key, so it can't apply a key change.
    IF UPDATE(MembershipID) AND EXISTS (SELECT 1 FROM deleted)
        THROW 50001, 'dbo.BPJSMembership: changing MembershipID is not supported', 1;

    -- New rows from apps: let IDENTITY generate the key. Guarded by IF EXISTS:
    -- when the sink inserts, IDENTITY_INSERT is ON for this session, and an
    -- INSERT without the key column then fails (error 545) even for zero rows.
    IF EXISTS (SELECT 1 FROM inserted i
               WHERE ISNULL(i.MembershipID, 0) = 0
                 AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.MembershipID = i.MembershipID))
        INSERT INTO dbo.BPJSMembership
            (EmployeeID, BPJSNumber, NIK, FullName, EnrollmentDate, WorkRiskGroup, IsActive, CreatedAt, created_by, updated_by, updated_at, deleted_by, deleted_at, db_login)
        SELECT i.EmployeeID, i.BPJSNumber, i.NIK, i.FullName, i.EnrollmentDate, i.WorkRiskGroup, i.IsActive, i.CreatedAt, i.created_by, i.updated_by, i.updated_at, i.deleted_by, i.deleted_at, ORIGINAL_LOGIN()
        FROM inserted i
        WHERE ISNULL(i.MembershipID, 0) = 0
          AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.MembershipID = i.MembershipID);

    -- New rows replicated from PostgreSQL: keep the incoming key.
    IF EXISTS (SELECT 1 FROM inserted i
               WHERE ISNULL(i.MembershipID, 0) <> 0
                 AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.MembershipID = i.MembershipID))
    BEGIN
        -- An explicit key moves IDENTITY up to the highest key inserted, which
        -- would push this node's own keys into PostgreSQL's range. Hold the
        -- table so no app insert draws a key meanwhile, then put the counter back.
        DECLARE @ident NUMERIC(38, 0);
        SELECT TOP (0) @ident = 0 FROM dbo.BPJSMembership WITH (TABLOCKX, HOLDLOCK);
        SET @ident = IDENT_CURRENT('dbo.BPJSMembership');
        BEGIN TRY
            SET IDENTITY_INSERT dbo.BPJSMembership ON;
            INSERT INTO dbo.BPJSMembership
                (MembershipID, EmployeeID, BPJSNumber, NIK, FullName, EnrollmentDate, WorkRiskGroup, IsActive, CreatedAt, created_by, updated_by, updated_at, deleted_by, deleted_at, db_login)
            SELECT i.MembershipID, i.EmployeeID, i.BPJSNumber, i.NIK, i.FullName, i.EnrollmentDate, i.WorkRiskGroup, i.IsActive, i.CreatedAt, i.created_by, i.updated_by, i.updated_at, i.deleted_by, i.deleted_at, ORIGINAL_LOGIN()
            FROM inserted i
            WHERE ISNULL(i.MembershipID, 0) <> 0
              AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.MembershipID = i.MembershipID);
            SET IDENTITY_INSERT dbo.BPJSMembership OFF;
        END TRY
        BEGIN CATCH
            -- The failed insert (e.g. a foreign key error) has already moved
            -- IDENTITY, and a rollback doesn't move it back. The error dooms the
            -- transaction, so roll back first, then reseed (that commits on its
            -- own), then re-raise so the writer still sees the original error.
            IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
            IF IDENT_CURRENT('dbo.BPJSMembership') <> @ident
                DBCC CHECKIDENT ('dbo.BPJSMembership', RESEED, @ident) WITH NO_INFOMSGS;
            THROW;
        END CATCH;
        IF IDENT_CURRENT('dbo.BPJSMembership') <> @ident
            DBCC CHECKIDENT ('dbo.BPJSMembership', RESEED, @ident) WITH NO_INFOMSGS;
    END;

    -- Updates that win last-writer-wins, and whether they change a unique column.
    -- (Compared as binary: = ignores case and trailing spaces under this collation.)
    DECLARE @win TABLE (id INT PRIMARY KEY, uniq_changed BIT NOT NULL);
    INSERT INTO @win (id, uniq_changed)
    SELECT i.MembershipID,
           CASE WHEN CAST(i.NIK AS VARBINARY(MAX)) <> CAST(d.NIK AS VARBINARY(MAX))
             OR CAST(i.BPJSNumber AS VARBINARY(MAX)) <> CAST(d.BPJSNumber AS VARBINARY(MAX))
                THEN 1 ELSE 0 END
    FROM inserted i
    JOIN deleted  d ON d.MembershipID = i.MembershipID
    WHERE COALESCE(CAST(i.updated_at AS DATETIME2(6)), CAST(i.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6))) > COALESCE(CAST(d.updated_at AS DATETIME2(6)), CAST(d.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6)))
       OR (COALESCE(CAST(i.updated_at AS DATETIME2(6)), CAST(i.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6))) = COALESCE(CAST(d.updated_at AS DATETIME2(6)), CAST(d.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6)))
           AND COALESCE(i.updated_by, i.created_by, -1) > COALESCE(d.updated_by, d.created_by, -1));

    -- A set-based UPDATE that SETs a unique-index column (NIK / BPJSNumber) runs as delete +
    -- insert, CDC records it that way, and the delete then replicates as a real
    -- delete of the row. So the common case leaves those columns out ...
    UPDATE t
    SET EmployeeID         = i.EmployeeID,
        FullName           = i.FullName,
        EnrollmentDate     = i.EnrollmentDate,
        WorkRiskGroup      = i.WorkRiskGroup,
        IsActive           = i.IsActive,
        CreatedAt          = i.CreatedAt,
        created_by         = i.created_by,
        updated_by         = i.updated_by,
        updated_at         = i.updated_at,
        deleted_by         = i.deleted_by,
        deleted_at         = i.deleted_at,
        db_login           = ORIGINAL_LOGIN()
    FROM dbo.BPJSMembership t
    JOIN inserted i ON i.MembershipID = t.MembershipID
    JOIN @win     w ON w.id = t.MembershipID AND w.uniq_changed = 0;

    -- ... and a row that does change one is updated on its own key, which SQL Server
    -- runs (and CDC records) as an update.
    DECLARE @id INT;
    DECLARE uniq_rows CURSOR LOCAL FAST_FORWARD FOR SELECT id FROM @win WHERE uniq_changed = 1;
    OPEN uniq_rows;
    FETCH NEXT FROM uniq_rows INTO @id;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        UPDATE t
        SET EmployeeID         = i.EmployeeID,
            BPJSNumber         = i.BPJSNumber,
            NIK                = i.NIK,
            FullName           = i.FullName,
            EnrollmentDate     = i.EnrollmentDate,
            WorkRiskGroup      = i.WorkRiskGroup,
            IsActive           = i.IsActive,
            CreatedAt          = i.CreatedAt,
            created_by         = i.created_by,
            updated_by         = i.updated_by,
            updated_at         = i.updated_at,
            deleted_by         = i.deleted_by,
            deleted_at         = i.deleted_at,
            db_login           = ORIGINAL_LOGIN()
        FROM dbo.BPJSMembership t
        JOIN inserted i ON i.MembershipID = t.MembershipID
        WHERE t.MembershipID = @id;
        FETCH NEXT FROM uniq_rows INTO @id;
    END;
    CLOSE uniq_rows;
    DEALLOCATE uniq_rows;
END;
GO

CREATE OR ALTER TRIGGER dbo.tr_BPJSMonthlyContribution_LWW ON dbo.BPJSMonthlyContribution
INSTEAD OF INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- This trigger joins inserted to deleted on the key, so it can't apply a key change.
    IF UPDATE(ContributionID) AND EXISTS (SELECT 1 FROM deleted)
        THROW 50001, 'dbo.BPJSMonthlyContribution: changing ContributionID is not supported', 1;

    -- New rows from apps: let IDENTITY generate the key. Guarded by IF EXISTS:
    -- when the sink inserts, IDENTITY_INSERT is ON for this session, and an
    -- INSERT without the key column then fails (error 545) even for zero rows.
    IF EXISTS (SELECT 1 FROM inserted i
               WHERE ISNULL(i.ContributionID, 0) = 0
                 AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ContributionID = i.ContributionID))
        INSERT INTO dbo.BPJSMonthlyContribution
            (MembershipID, PeriodMonth, PeriodYear, BaseWage, JHTEmployee, JHTEmployer, JKKEmployer, JKMEmployer, JPEmployee, JPEmployer, PaymentStatus, PaidAt, CreatedAt, created_by, updated_by, updated_at, deleted_by, deleted_at, db_login)
        SELECT i.MembershipID, i.PeriodMonth, i.PeriodYear, i.BaseWage, i.JHTEmployee, i.JHTEmployer, i.JKKEmployer, i.JKMEmployer, i.JPEmployee, i.JPEmployer, i.PaymentStatus, i.PaidAt, i.CreatedAt, i.created_by, i.updated_by, i.updated_at, i.deleted_by, i.deleted_at, ORIGINAL_LOGIN()
        FROM inserted i
        WHERE ISNULL(i.ContributionID, 0) = 0
          AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ContributionID = i.ContributionID);

    -- New rows replicated from PostgreSQL: keep the incoming key.
    IF EXISTS (SELECT 1 FROM inserted i
               WHERE ISNULL(i.ContributionID, 0) <> 0
                 AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ContributionID = i.ContributionID))
    BEGIN
        -- An explicit key moves IDENTITY up to the highest key inserted, which
        -- would push this node's own keys into PostgreSQL's range. Hold the
        -- table so no app insert draws a key meanwhile, then put the counter back.
        DECLARE @ident NUMERIC(38, 0);
        SELECT TOP (0) @ident = 0 FROM dbo.BPJSMonthlyContribution WITH (TABLOCKX, HOLDLOCK);
        SET @ident = IDENT_CURRENT('dbo.BPJSMonthlyContribution');
        BEGIN TRY
            SET IDENTITY_INSERT dbo.BPJSMonthlyContribution ON;
            INSERT INTO dbo.BPJSMonthlyContribution
                (ContributionID, MembershipID, PeriodMonth, PeriodYear, BaseWage, JHTEmployee, JHTEmployer, JKKEmployer, JKMEmployer, JPEmployee, JPEmployer, PaymentStatus, PaidAt, CreatedAt, created_by, updated_by, updated_at, deleted_by, deleted_at, db_login)
            SELECT i.ContributionID, i.MembershipID, i.PeriodMonth, i.PeriodYear, i.BaseWage, i.JHTEmployee, i.JHTEmployer, i.JKKEmployer, i.JKMEmployer, i.JPEmployee, i.JPEmployer, i.PaymentStatus, i.PaidAt, i.CreatedAt, i.created_by, i.updated_by, i.updated_at, i.deleted_by, i.deleted_at, ORIGINAL_LOGIN()
            FROM inserted i
            WHERE ISNULL(i.ContributionID, 0) <> 0
              AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ContributionID = i.ContributionID);
            SET IDENTITY_INSERT dbo.BPJSMonthlyContribution OFF;
        END TRY
        BEGIN CATCH
            -- The failed insert (e.g. a foreign key error) has already moved
            -- IDENTITY, and a rollback doesn't move it back. The error dooms the
            -- transaction, so roll back first, then reseed (that commits on its
            -- own), then re-raise so the writer still sees the original error.
            IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
            IF IDENT_CURRENT('dbo.BPJSMonthlyContribution') <> @ident
                DBCC CHECKIDENT ('dbo.BPJSMonthlyContribution', RESEED, @ident) WITH NO_INFOMSGS;
            THROW;
        END CATCH;
        IF IDENT_CURRENT('dbo.BPJSMonthlyContribution') <> @ident
            DBCC CHECKIDENT ('dbo.BPJSMonthlyContribution', RESEED, @ident) WITH NO_INFOMSGS;
    END;

    -- Updates that win last-writer-wins, and whether they change the unique
    -- period key (MembershipID, PeriodYear, PeriodMonth).
    DECLARE @win TABLE (id INT PRIMARY KEY, uniq_changed BIT NOT NULL);
    INSERT INTO @win (id, uniq_changed)
    SELECT i.ContributionID,
           CASE WHEN i.MembershipID <> d.MembershipID
             OR i.PeriodYear <> d.PeriodYear
             OR i.PeriodMonth <> d.PeriodMonth
                THEN 1 ELSE 0 END
    FROM inserted i
    JOIN deleted  d ON d.ContributionID = i.ContributionID
    WHERE COALESCE(CAST(i.updated_at AS DATETIME2(6)), CAST(i.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6))) > COALESCE(CAST(d.updated_at AS DATETIME2(6)), CAST(d.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6)))
       OR (COALESCE(CAST(i.updated_at AS DATETIME2(6)), CAST(i.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6))) = COALESCE(CAST(d.updated_at AS DATETIME2(6)), CAST(d.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6)))
           AND COALESCE(i.updated_by, i.created_by, -1) > COALESCE(d.updated_by, d.created_by, -1));

    -- A set-based UPDATE that SETs a unique-index column runs as delete + insert,
    -- CDC records it that way, and the delete then replicates as a real delete of
    -- the row. So the common case leaves the period key columns out ...
    UPDATE t
    SET BaseWage           = i.BaseWage,
        JHTEmployee        = i.JHTEmployee,
        JHTEmployer        = i.JHTEmployer,
        JKKEmployer        = i.JKKEmployer,
        JKMEmployer        = i.JKMEmployer,
        JPEmployee         = i.JPEmployee,
        JPEmployer         = i.JPEmployer,
        PaymentStatus      = i.PaymentStatus,
        PaidAt             = i.PaidAt,
        CreatedAt          = i.CreatedAt,
        created_by         = i.created_by,
        updated_by         = i.updated_by,
        updated_at         = i.updated_at,
        deleted_by         = i.deleted_by,
        deleted_at         = i.deleted_at,
        db_login           = ORIGINAL_LOGIN()
    FROM dbo.BPJSMonthlyContribution t
    JOIN inserted i ON i.ContributionID = t.ContributionID
    JOIN @win     w ON w.id = t.ContributionID AND w.uniq_changed = 0;

    -- ... and a row that does change one is updated on its own key, which SQL Server
    -- runs (and CDC records) as an update.
    DECLARE @id INT;
    DECLARE uniq_rows CURSOR LOCAL FAST_FORWARD FOR SELECT id FROM @win WHERE uniq_changed = 1;
    OPEN uniq_rows;
    FETCH NEXT FROM uniq_rows INTO @id;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        UPDATE t
        SET MembershipID       = i.MembershipID,
            PeriodMonth        = i.PeriodMonth,
            PeriodYear         = i.PeriodYear,
            BaseWage           = i.BaseWage,
            JHTEmployee        = i.JHTEmployee,
            JHTEmployer        = i.JHTEmployer,
            JKKEmployer        = i.JKKEmployer,
            JKMEmployer        = i.JKMEmployer,
            JPEmployee         = i.JPEmployee,
            JPEmployer         = i.JPEmployer,
            PaymentStatus      = i.PaymentStatus,
            PaidAt             = i.PaidAt,
            CreatedAt          = i.CreatedAt,
            created_by         = i.created_by,
            updated_by         = i.updated_by,
            updated_at         = i.updated_at,
            deleted_by         = i.deleted_by,
            deleted_at         = i.deleted_at,
            db_login           = ORIGINAL_LOGIN()
        FROM dbo.BPJSMonthlyContribution t
        JOIN inserted i ON i.ContributionID = t.ContributionID
        WHERE t.ContributionID = @id;
        FETCH NEXT FROM uniq_rows INTO @id;
    END;
    CLOSE uniq_rows;
    DEALLOCATE uniq_rows;
END;
GO

CREATE OR ALTER TRIGGER dbo.tr_BPJSClaim_LWW ON dbo.BPJSClaim
INSTEAD OF INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- This trigger joins inserted to deleted on the key, so it can't apply a key change.
    IF UPDATE(ClaimID) AND EXISTS (SELECT 1 FROM deleted)
        THROW 50001, 'dbo.BPJSClaim: changing ClaimID is not supported', 1;

    -- New rows from apps: let IDENTITY generate the key. Guarded by IF EXISTS:
    -- when the sink inserts, IDENTITY_INSERT is ON for this session, and an
    -- INSERT without the key column then fails (error 545) even for zero rows.
    IF EXISTS (SELECT 1 FROM inserted i
               WHERE ISNULL(i.ClaimID, 0) = 0
                 AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ClaimID = i.ClaimID))
        INSERT INTO dbo.BPJSClaim
            (MembershipID, ClaimNumber, ProgramType, ClaimDate, ReasonDescription, DisbursedAmount, ClaimStatus, ApprovedAt, CreatedAt, created_by, updated_by, updated_at, deleted_by, deleted_at, db_login)
        SELECT i.MembershipID, i.ClaimNumber, i.ProgramType, i.ClaimDate, i.ReasonDescription, i.DisbursedAmount, i.ClaimStatus, i.ApprovedAt, i.CreatedAt, i.created_by, i.updated_by, i.updated_at, i.deleted_by, i.deleted_at, ORIGINAL_LOGIN()
        FROM inserted i
        WHERE ISNULL(i.ClaimID, 0) = 0
          AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ClaimID = i.ClaimID);

    -- New rows replicated from PostgreSQL: keep the incoming key.
    IF EXISTS (SELECT 1 FROM inserted i
               WHERE ISNULL(i.ClaimID, 0) <> 0
                 AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ClaimID = i.ClaimID))
    BEGIN
        -- An explicit key moves IDENTITY up to the highest key inserted, which
        -- would push this node's own keys into PostgreSQL's range. Hold the
        -- table so no app insert draws a key meanwhile, then put the counter back.
        DECLARE @ident NUMERIC(38, 0);
        SELECT TOP (0) @ident = 0 FROM dbo.BPJSClaim WITH (TABLOCKX, HOLDLOCK);
        SET @ident = IDENT_CURRENT('dbo.BPJSClaim');
        BEGIN TRY
            SET IDENTITY_INSERT dbo.BPJSClaim ON;
            INSERT INTO dbo.BPJSClaim
                (ClaimID, MembershipID, ClaimNumber, ProgramType, ClaimDate, ReasonDescription, DisbursedAmount, ClaimStatus, ApprovedAt, CreatedAt, created_by, updated_by, updated_at, deleted_by, deleted_at, db_login)
            SELECT i.ClaimID, i.MembershipID, i.ClaimNumber, i.ProgramType, i.ClaimDate, i.ReasonDescription, i.DisbursedAmount, i.ClaimStatus, i.ApprovedAt, i.CreatedAt, i.created_by, i.updated_by, i.updated_at, i.deleted_by, i.deleted_at, ORIGINAL_LOGIN()
            FROM inserted i
            WHERE ISNULL(i.ClaimID, 0) <> 0
              AND NOT EXISTS (SELECT 1 FROM deleted d WHERE d.ClaimID = i.ClaimID);
            SET IDENTITY_INSERT dbo.BPJSClaim OFF;
        END TRY
        BEGIN CATCH
            -- The failed insert (e.g. a foreign key error) has already moved
            -- IDENTITY, and a rollback doesn't move it back. The error dooms the
            -- transaction, so roll back first, then reseed (that commits on its
            -- own), then re-raise so the writer still sees the original error.
            IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
            IF IDENT_CURRENT('dbo.BPJSClaim') <> @ident
                DBCC CHECKIDENT ('dbo.BPJSClaim', RESEED, @ident) WITH NO_INFOMSGS;
            THROW;
        END CATCH;
        IF IDENT_CURRENT('dbo.BPJSClaim') <> @ident
            DBCC CHECKIDENT ('dbo.BPJSClaim', RESEED, @ident) WITH NO_INFOMSGS;
    END;

    -- Updates that win last-writer-wins, and whether they change a unique column.
    -- (Compared as binary: = ignores case and trailing spaces under this collation.)
    DECLARE @win TABLE (id INT PRIMARY KEY, uniq_changed BIT NOT NULL);
    INSERT INTO @win (id, uniq_changed)
    SELECT i.ClaimID,
           CASE WHEN CAST(i.ClaimNumber AS VARBINARY(MAX)) <> CAST(d.ClaimNumber AS VARBINARY(MAX))
                THEN 1 ELSE 0 END
    FROM inserted i
    JOIN deleted  d ON d.ClaimID = i.ClaimID
    WHERE COALESCE(CAST(i.updated_at AS DATETIME2(6)), CAST(i.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6))) > COALESCE(CAST(d.updated_at AS DATETIME2(6)), CAST(d.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6)))
       OR (COALESCE(CAST(i.updated_at AS DATETIME2(6)), CAST(i.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6))) = COALESCE(CAST(d.updated_at AS DATETIME2(6)), CAST(d.CreatedAt AS DATETIME2(6)), CAST('0001-01-01' AS DATETIME2(6)))
           AND COALESCE(i.updated_by, i.created_by, -1) > COALESCE(d.updated_by, d.created_by, -1));

    -- A set-based UPDATE that SETs a unique-index column (ClaimNumber) runs as delete +
    -- insert, CDC records it that way, and the delete then replicates as a real
    -- delete of the row. So the common case leaves those columns out ...
    UPDATE t
    SET MembershipID       = i.MembershipID,
        ProgramType        = i.ProgramType,
        ClaimDate          = i.ClaimDate,
        ReasonDescription  = i.ReasonDescription,
        DisbursedAmount    = i.DisbursedAmount,
        ClaimStatus        = i.ClaimStatus,
        ApprovedAt         = i.ApprovedAt,
        CreatedAt          = i.CreatedAt,
        created_by         = i.created_by,
        updated_by         = i.updated_by,
        updated_at         = i.updated_at,
        deleted_by         = i.deleted_by,
        deleted_at         = i.deleted_at,
        db_login           = ORIGINAL_LOGIN()
    FROM dbo.BPJSClaim t
    JOIN inserted i ON i.ClaimID = t.ClaimID
    JOIN @win     w ON w.id = t.ClaimID AND w.uniq_changed = 0;

    -- ... and a row that does change one is updated on its own key, which SQL Server
    -- runs (and CDC records) as an update.
    DECLARE @id INT;
    DECLARE uniq_rows CURSOR LOCAL FAST_FORWARD FOR SELECT id FROM @win WHERE uniq_changed = 1;
    OPEN uniq_rows;
    FETCH NEXT FROM uniq_rows INTO @id;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        UPDATE t
        SET MembershipID       = i.MembershipID,
            ClaimNumber        = i.ClaimNumber,
            ProgramType        = i.ProgramType,
            ClaimDate          = i.ClaimDate,
            ReasonDescription  = i.ReasonDescription,
            DisbursedAmount    = i.DisbursedAmount,
            ClaimStatus        = i.ClaimStatus,
            ApprovedAt         = i.ApprovedAt,
            CreatedAt          = i.CreatedAt,
            created_by         = i.created_by,
            updated_by         = i.updated_by,
            updated_at         = i.updated_at,
            deleted_by         = i.deleted_by,
            deleted_at         = i.deleted_at,
            db_login           = ORIGINAL_LOGIN()
        FROM dbo.BPJSClaim t
        JOIN inserted i ON i.ClaimID = t.ClaimID
        WHERE t.ClaimID = @id;
        FETCH NEXT FROM uniq_rows INTO @id;
    END;
    CLOSE uniq_rows;
    DEALLOCATE uniq_rows;
END;
GO
