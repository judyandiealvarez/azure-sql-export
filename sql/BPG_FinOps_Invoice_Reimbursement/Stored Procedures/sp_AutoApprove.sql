
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
