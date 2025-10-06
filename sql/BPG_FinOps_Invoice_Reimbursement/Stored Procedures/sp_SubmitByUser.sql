
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_SubmitByUser]
(
    @ContactEmail nvarchar(250),
	@QuarterId uniqueidentifier
)
AS
BEGIN
    SET NOCOUNT ON

	update c
	set IsSubmitted = 1
    from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] c
	where c.QuarterId = @QuarterId
	and c.SentForApproval = 3
	and c.ApprovalStatus <> 0
	and c.IsSubmitted = 0
	and @ContactEmail in 
	(
			select [Email] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectMember] m
			where m.ProjectId = c.ProjectId and m.QuarterId = c.QuarterId
	)
END
