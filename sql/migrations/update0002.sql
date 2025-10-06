-- Summary
-- Schema: BPG_FinOps_Invoice_Reimbursement
-- | Type | Created | Updated | Dropped |
-- |------|---------:|---------:|---------:|
-- | Tables | 0 | 0 | 0 |
-- | Views | 0 | 0 | 0 |
-- | StoredProcedures | 0 | 1 | 0 |
-- | Functions | 0 | 0 | 0 |
-- | Triggers | 0 | 0 | 0 |
--
-- Details
-- | Change | Type | Object |
-- |--------|------|--------|
-- | Updated | StoredProcedures | sp_AutoApprove |
-- Generated at 2025-10-06T11:23:52.856581Z

-- Update StoredProcedure: sp_AutoApprove

-- =============================================
-- Author:      Judy Alvarez
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_AutoApprove]
AS
BEGIN
    SET NOCOUNT ON

    update c
	set c.ApprovalStatus = 3, c.IsAutoApproved = 1, c.IsSubmitted = 1
	from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] c
	where c.SentForApproval = 3
	and c.IsSubmitted = 0
	and c.ApprovalStatus = 0
	and DATEADD(day, 15, c.SentForApprovalDate) > GETUTCDATE()
END

GO
