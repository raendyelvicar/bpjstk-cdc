-- Tables for the SQL Server node (bpjsk_poc). Run first, before
-- mssql-02-enable-cdc.sql and mssql-03-lww-trigger.sql. Safe to re-run: the
-- database and each table are only created when missing (existing tables are
-- left as they are, so this does not migrate a changed definition).
--
-- Must stay in step with postgres-01-schema.sql: same columns (PascalCase here,
-- snake_case there; the sinks' ReplaceField renames map one to the other),
-- compatible types and nullability, same unique keys, same TotalAmount formula.
--
-- Keys: IDENTITY generates 1..999999999 on this node; PostgreSQL generates
-- >= 1000000000 (postgres-02-replication.sql). The LWW trigger keeps IDENTITY
-- below that range after the sink inserts PostgreSQL keys.
-- Timestamps are UTC (SYSUTCDATETIME()), DATETIME2(6) to match PostgreSQL's
-- microseconds. Audit users: SQL Server < 1000000, PostgreSQL >= 1000000.
IF DB_ID('bpjsk_poc') IS NULL
    CREATE DATABASE [bpjsk_poc];
GO
USE [bpjsk_poc];
GO
-- Needed for the persisted computed TotalAmount column (and saved with each
-- trigger). sqlcmd defaults both to OFF; SSMS and most GUI clients to ON.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

IF OBJECT_ID('dbo.BPJSMembership', 'U') IS NULL
CREATE TABLE dbo.BPJSMembership (
    MembershipID    INT IDENTITY(1, 1) NOT NULL,
    EmployeeID      INT            NOT NULL,
    BPJSNumber      NVARCHAR(20)   NOT NULL,
    NIK             NVARCHAR(16)   NOT NULL,
    FullName        NVARCHAR(150)  NOT NULL,
    EnrollmentDate  DATE           NOT NULL,
    WorkRiskGroup   INT            NOT NULL CONSTRAINT DF_BPJSMembership_WorkRiskGroup DEFAULT (1),
    IsActive        BIT            NOT NULL CONSTRAINT DF_BPJSMembership_IsActive DEFAULT (1),
    CreatedAt       DATETIME2(6)   NULL     CONSTRAINT DF_BPJSMembership_CreatedAt DEFAULT (SYSUTCDATETIME()),
    created_by      INT            NULL,
    updated_by      INT            NULL,
    updated_at      DATETIME2(6)   NULL,
    deleted_by      INT            NULL,
    deleted_at      DATETIME2(6)   NULL,
    CONSTRAINT PK_BPJSMembership PRIMARY KEY (MembershipID),
    CONSTRAINT UQ_BPJSMembership_BPJSNumber UNIQUE (BPJSNumber),
    CONSTRAINT UQ_BPJSMembership_NIK UNIQUE (NIK)
);
GO

IF OBJECT_ID('dbo.BPJSMonthlyContribution', 'U') IS NULL
CREATE TABLE dbo.BPJSMonthlyContribution (
    ContributionID  INT IDENTITY(1, 1) NOT NULL,
    MembershipID    INT            NOT NULL,
    PeriodMonth     SMALLINT       NOT NULL,
    PeriodYear      SMALLINT       NOT NULL,
    BaseWage        DECIMAL(18, 2) NOT NULL,
    JHTEmployee     DECIMAL(18, 2) NOT NULL CONSTRAINT DF_BPJSMonthlyContribution_JHTEmployee DEFAULT (0),
    JHTEmployer     DECIMAL(18, 2) NOT NULL CONSTRAINT DF_BPJSMonthlyContribution_JHTEmployer DEFAULT (0),
    JKKEmployer     DECIMAL(18, 2) NOT NULL CONSTRAINT DF_BPJSMonthlyContribution_JKKEmployer DEFAULT (0),
    JKMEmployer     DECIMAL(18, 2) NOT NULL CONSTRAINT DF_BPJSMonthlyContribution_JKMEmployer DEFAULT (0),
    JPEmployee      DECIMAL(18, 2) NOT NULL CONSTRAINT DF_BPJSMonthlyContribution_JPEmployee DEFAULT (0),
    JPEmployer      DECIMAL(18, 2) NOT NULL CONSTRAINT DF_BPJSMonthlyContribution_JPEmployer DEFAULT (0),
    -- Computed on each node and never replicated (CDC reports it as NULL; both
    -- sinks drop it). Same formula as total_amount in postgres-01-schema.sql.
    TotalAmount     AS (JHTEmployee + JHTEmployer + JKKEmployer + JKMEmployer + JPEmployee + JPEmployer) PERSISTED,
    PaymentStatus   NVARCHAR(30)   NOT NULL CONSTRAINT DF_BPJSMonthlyContribution_PaymentStatus DEFAULT (N'UNPAID'),
    PaidAt          DATETIME2(6)   NULL,
    CreatedAt       DATETIME2(6)   NULL     CONSTRAINT DF_BPJSMonthlyContribution_CreatedAt DEFAULT (SYSUTCDATETIME()),
    created_by      INT            NULL,
    updated_by      INT            NULL,
    updated_at      DATETIME2(6)   NULL,
    deleted_by      INT            NULL,
    deleted_at      DATETIME2(6)   NULL,
    CONSTRAINT PK_BPJSMonthlyContribution PRIMARY KEY (ContributionID),
    CONSTRAINT UQ_BPJSMonthlyContribution_Period UNIQUE (MembershipID, PeriodYear, PeriodMonth),
    CONSTRAINT CK_BPJSMonthlyContribution_PeriodMonth CHECK (PeriodMonth BETWEEN 1 AND 12),
    CONSTRAINT FK_BPJSMonthlyContribution_Membership FOREIGN KEY (MembershipID)
        REFERENCES dbo.BPJSMembership (MembershipID)
);
GO

IF OBJECT_ID('dbo.BPJSClaim', 'U') IS NULL
CREATE TABLE dbo.BPJSClaim (
    ClaimID           INT IDENTITY(1, 1) NOT NULL,
    MembershipID      INT            NOT NULL,
    ClaimNumber       NVARCHAR(30)   NOT NULL,
    ProgramType       NVARCHAR(10)   NOT NULL,   -- JHT, JKK, JKM, JP, JKP
    ClaimDate         DATE           NOT NULL,
    ReasonDescription NVARCHAR(500)  NOT NULL,
    DisbursedAmount   DECIMAL(18, 2) NULL,
    ClaimStatus       NVARCHAR(30)   NOT NULL CONSTRAINT DF_BPJSClaim_ClaimStatus DEFAULT (N'SUBMITTED'),
    ApprovedAt        DATETIME2(6)   NULL,
    CreatedAt         DATETIME2(6)   NULL     CONSTRAINT DF_BPJSClaim_CreatedAt DEFAULT (SYSUTCDATETIME()),
    created_by        INT            NULL,
    updated_by        INT            NULL,
    updated_at        DATETIME2(6)   NULL,
    deleted_by        INT            NULL,
    deleted_at        DATETIME2(6)   NULL,
    CONSTRAINT PK_BPJSClaim PRIMARY KEY (ClaimID),
    CONSTRAINT UQ_BPJSClaim_ClaimNumber UNIQUE (ClaimNumber),
    CONSTRAINT FK_BPJSClaim_Membership FOREIGN KEY (MembershipID)
        REFERENCES dbo.BPJSMembership (MembershipID)
);
GO

-- FK columns are not indexed automatically.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_BPJSClaim_MembershipID' AND object_id = OBJECT_ID('dbo.BPJSClaim'))
    CREATE INDEX IX_BPJSClaim_MembershipID ON dbo.BPJSClaim (MembershipID);
GO
-- (BPJSMonthlyContribution.MembershipID is covered by UQ_BPJSMonthlyContribution_Period.)
