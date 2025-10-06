
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadQuarterData]
(
	@ImportId uniqueidentifier
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    begin transaction

	declare @QuarterId uniqueidentifier

	set @QuarterId = (select top 1 QuarterId from [BPG_FinOps_Invoice_Reimbursement].[ImportSession] where ImportId = @ImportId)
	
	EXECUTE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadQuarterMPLData] @ImportId
	EXECUTE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadQuarterPPTData] @ImportId
	EXECUTE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadGrouping] @QuarterId

	commit transaction
END
