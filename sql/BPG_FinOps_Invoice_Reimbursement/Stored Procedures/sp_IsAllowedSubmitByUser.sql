
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_IsAllowedSubmitByUser]
(
    @ContactEmail nvarchar(250),
	@QuarterId uniqueidentifier,
	@Allowed bit OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON

	INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[Log]
           ([Source]
           ,[Description])
     VALUES
           ('[BPG_FinOps_Invoice_Reimbursement].[sp_IsAllowedSubmitByUser]'
           ,'Begin args: @ContactEmail = ''' 
		    + @ContactEmail + ''' @Quarter = '''
		    + cast(isnull(@QuarterId, 'NULL') as NVARCHAR(50)) + '''')

	declare @cnt int

    set @cnt = (
		select count(Id) from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] c
		where c.QuarterId = @QuarterId
		and c.SentForApproval = 3
		and c.ApprovalStatus = 0
		and c.IsSubmitted = 0
		and @ContactEmail in 
		(
				select [Email] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectMember] m
				where m.ProjectId = c.ProjectId and m.QuarterId = c.QuarterId
		)
	)

	set @Allowed = case when @cnt > 0 then 0 else 1 end

	return @cnt
END
