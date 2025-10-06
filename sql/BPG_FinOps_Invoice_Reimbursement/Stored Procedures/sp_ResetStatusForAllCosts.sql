-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_ResetStatusForAllCosts]
AS
BEGIN
    SET NOCOUNT ON

    update c set SentForApproval = 0, ApprovalStatus = 0, IsSubmitted = 0

	from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] c
END
