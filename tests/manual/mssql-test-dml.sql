-- Manual replication test, SQL Server side (BPJSTK_POC). Run one step at a time
-- and check PostgreSQL (and the Kafka topics) before the next step.
--
-- Writer rules (see sql/mssql-03-lww-trigger.sql):
--   * created_by / updated_by: SQL Server users < 1000000 (here 101), or the
--     ksqlDB loop filter drops the change and it never reaches PostgreSQL.
--   * every UPDATE must set a newer updated_at, or the LWW trigger skips it.
--   * timestamps in UTC (SYSUTCDATETIME()).
--   * leave the ID columns out: the INSTEAD OF trigger lets IDENTITY generate
--     them, so SCOPE_IDENTITY() doesn't return them. Look rows up by BPJSNumber /
--     ClaimNumber instead.
-- Test rows: BPJS number CDCT2..., NIK 992..., claim number CDCT-MS-...
-- (tests/test_cdc.py cleanup also deletes CDCT rows).
-- CDC capture runs on SQL Server Agent, so allow a few seconds per step.
USE BPJSTK_POC;
GO

-- ============================================================ 1. INSERT parent
INSERT INTO dbo.BPJSMembership
    (EmployeeID, BPJSNumber, NIK, FullName, EnrollmentDate, CreatedAt, created_by)
VALUES
    (920001, 'CDCT200000000000001', '9920000000000001', 'MS TEST NEW', '2024-01-01', SYSUTCDATETIME(), 101);

SELECT MembershipID, FullName, created_by FROM dbo.BPJSMembership
WHERE BPJSNumber = 'CDCT200000000000001';
GO
-- Verify on PostgreSQL:
--   SELECT membership_id, full_name, created_by FROM public.bpjs_membership WHERE bpjs_number = 'CDCT200000000000001';
-- Wait until the row is there before step 2: the children reference it, and
-- the sink can apply topics in any order.

-- ============================================================ 2. INSERT children
INSERT INTO dbo.BPJSMonthlyContribution
    (MembershipID, PeriodMonth, PeriodYear, BaseWage, JHTEmployee, JHTEmployer,
     JKKEmployer, JKMEmployer, JPEmployee, JPEmployer, PaymentStatus, CreatedAt, created_by)
SELECT MembershipID, 2, 2099, 5000000.00, 100000.00, 185000.00,
       12000.00, 15000.00, 50000.00, 100000.00, 'NEW', SYSUTCDATETIME(), 101
FROM dbo.BPJSMembership WHERE BPJSNumber = 'CDCT200000000000001';

INSERT INTO dbo.BPJSClaim
    (MembershipID, ClaimNumber, ProgramType, ClaimDate, ReasonDescription, ClaimStatus, CreatedAt, created_by)
SELECT MembershipID, 'CDCT-MS-0001', 'JHT', '2025-06-01', 'CDC manual test', 'NEW', SYSUTCDATETIME(), 101
FROM dbo.BPJSMembership WHERE BPJSNumber = 'CDCT200000000000001';
GO
-- Verify on PostgreSQL:
--   SELECT c.contribution_id, c.payment_status, c.total_amount FROM public.bpjs_monthly_contribution c
--     JOIN public.bpjs_membership m USING (membership_id) WHERE m.bpjs_number = 'CDCT200000000000001';
--   SELECT claim_id, claim_status FROM public.bpjs_claim WHERE claim_number = 'CDCT-MS-0001';

-- ============================================================ 3. UPDATE
UPDATE dbo.BPJSMembership
SET FullName = 'MS TEST UPDATED', updated_at = SYSUTCDATETIME(), updated_by = 101
WHERE BPJSNumber = 'CDCT200000000000001';

UPDATE c
SET PaymentStatus = 'PAID', PaidAt = SYSUTCDATETIME(), updated_at = SYSUTCDATETIME(), updated_by = 101
FROM dbo.BPJSMonthlyContribution c
JOIN dbo.BPJSMembership m ON m.MembershipID = c.MembershipID
WHERE m.BPJSNumber = 'CDCT200000000000001';

UPDATE dbo.BPJSClaim
SET ClaimStatus = 'APPROVED', ApprovedAt = SYSUTCDATETIME(), DisbursedAmount = 25000000.00,
    updated_at = SYSUTCDATETIME(), updated_by = 101
WHERE ClaimNumber = 'CDCT-MS-0001';
GO
-- Verify on PostgreSQL: full_name = 'MS TEST UPDATED', payment_status = 'PAID',
-- claim_status = 'APPROVED', updated_by = 101 on all three.

-- ============================================================ 4. DELETE (children first)
DELETE FROM dbo.BPJSClaim WHERE ClaimNumber = 'CDCT-MS-0001';

DELETE c
FROM dbo.BPJSMonthlyContribution c
JOIN dbo.BPJSMembership m ON m.MembershipID = c.MembershipID
WHERE m.BPJSNumber = 'CDCT200000000000001';
GO
-- Wait for the two child deletes to reach PostgreSQL before deleting the parent.
DELETE FROM dbo.BPJSMembership WHERE BPJSNumber = 'CDCT200000000000001';
GO
-- Verify on PostgreSQL: the three SELECTs above return no rows.
