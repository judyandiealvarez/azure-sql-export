
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadGrouping]
(
    @QuarterId uniqueidentifier,
	@Cleanup bit = 0
)
AS
BEGIN
    SET NOCOUNT ON

	if @Cleanup = 1 
	begin
	    delete from [BPG_FinOps_Invoice_Reimbursement].[ProjectGrouping]
		    where [QuarterId] = @QuarterId

		delete from [BPG_FinOps_Invoice_Reimbursement].[GroupingMember]
		    where [QuarterId] = @QuarterId
	end

	INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[ProjectGrouping]
           ([QuarterId]
           ,[ProjectId]
           ,[GroupingCode])
     
	select @QuarterId
	      ,[project_code]
		  ,[grouping]
	FROM [BPG_FinOps_Invoice_Reimbursement].[v_ProjectGrouping]
	where [grouping] is not null 
	and [grouping] <> ''

	INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[GroupingMember]
           ([GroupingCode]
           ,[Email]
           ,[QuarterId])
	SELECT [grouping]
           ,[contact]
		   ,@QuarterId
    FROM [BPG_FinOps_Invoice_Reimbursement].[v_GroupingContacts]
	where [grouping] is not null 
	and [grouping] <> ''
	and [contact] is not null 
	and [contact] <> ''
	and [contact] <> 'TBD'
	and [contact] <> 'N/A'
END
