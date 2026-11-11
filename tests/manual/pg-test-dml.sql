-- Manual replication test, PostgreSQL side (bpjstk_poc). Run one step at a time
-- and check SQL Server (and the Kafka topics) before the next step.
--
-- Writer rules (see sql/postgres-03-lww-trigger.sql):
--   * created_by / updated_by: PostgreSQL users >= 1000000 (here 1000101), or the
--     ksqlDB loop filter drops the change and it never reaches SQL Server.
--   * every UPDATE must set a newer updated_at, or the LWW trigger skips it.
-- Test rows: BPJS number CDCT1..., NIK 991..., claim number CDCT-PG-...
-- (tests/test_cdc.py cleanup also deletes CDCT rows).

-- ============================================================ 1. INSERT parent
INSERT INTO public.bpjs_membership
    (employee_id, bpjs_number, nik, full_name, enrollment_date, created_at, created_by)
VALUES
    (910001, 'CDCT100000000000001', '9910000000000001', 'PG TEST NEW', DATE '2024-01-01', now(), 1000101);

SELECT membership_id, full_name, created_by FROM public.bpjs_membership
WHERE bpjs_number = 'CDCT100000000000001';
-- Verify on SQL Server (within a few seconds):
--   SELECT MembershipID, FullName, created_by FROM dbo.BPJSMembership WHERE BPJSNumber = 'CDCT100000000000001';
-- Wait until the row is there before step 2: the children reference it, and
-- the sink can apply topics in any order.

-- ============================================================ 2. INSERT children
INSERT INTO public.bpjs_monthly_contribution
    (membership_id, period_month, period_year, base_wage, jht_employee, jht_employer,
     jkk_employer, jkm_employer, jp_employee, jp_employer, payment_status, created_at, created_by)
SELECT membership_id, 1, 2099, 5000000.00, 100000.00, 185000.00,
       12000.00, 15000.00, 50000.00, 100000.00, 'NEW', now(), 1000101
FROM public.bpjs_membership WHERE bpjs_number = 'CDCT100000000000001';

INSERT INTO public.bpjs_claim
    (membership_id, claim_number, program_type, claim_date, reason_description, claim_status, created_at, created_by)
SELECT membership_id, 'CDCT-PG-0001', 'JHT', DATE '2025-06-01', 'CDC manual test', 'NEW', now(), 1000101
FROM public.bpjs_membership WHERE bpjs_number = 'CDCT100000000000001';

-- Verify on SQL Server:
--   SELECT c.ContributionID, c.PaymentStatus, c.TotalAmount FROM dbo.BPJSMonthlyContribution c
--     JOIN dbo.BPJSMembership m ON m.MembershipID = c.MembershipID WHERE m.BPJSNumber = 'CDCT100000000000001';
--   SELECT ClaimID, ClaimStatus FROM dbo.BPJSClaim WHERE ClaimNumber = 'CDCT-PG-0001';

-- ============================================================ 3. UPDATE
UPDATE public.bpjs_membership
SET full_name = 'PG TEST UPDATED', updated_at = now(), updated_by = 1000101
WHERE bpjs_number = 'CDCT100000000000001';

UPDATE public.bpjs_monthly_contribution c
SET payment_status = 'PAID', paid_at = now(), updated_at = now(), updated_by = 1000101
FROM public.bpjs_membership m
WHERE m.membership_id = c.membership_id AND m.bpjs_number = 'CDCT100000000000001';

UPDATE public.bpjs_claim
SET claim_status = 'APPROVED', approved_at = now(), disbursed_amount = 25000000.00,
    updated_at = now(), updated_by = 1000101
WHERE claim_number = 'CDCT-PG-0001';

-- Verify on SQL Server: FullName = 'PG TEST UPDATED', PaymentStatus = 'PAID',
-- ClaimStatus = 'APPROVED', updated_by = 1000101 on all three.

-- ============================================================ 4. DELETE (children first)
DELETE FROM public.bpjs_claim WHERE claim_number = 'CDCT-PG-0001';

DELETE FROM public.bpjs_monthly_contribution c
USING public.bpjs_membership m
WHERE m.membership_id = c.membership_id AND m.bpjs_number = 'CDCT100000000000001';

-- Wait for the two child deletes to reach SQL Server before deleting the parent.
DELETE FROM public.bpjs_membership WHERE bpjs_number = 'CDCT100000000000001';

-- Verify on SQL Server: the three SELECTs above return no rows.
