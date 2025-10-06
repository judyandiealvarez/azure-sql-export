-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_ResetQuarter]
(
    @QuarterId NVARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON

    begin transaction

	delete from [BPG_FinOps_Invoice_Reimbursement].[ProjectGrouping]
		where [QuarterId] = @QuarterId

	delete from [BPG_FinOps_Invoice_Reimbursement].[GroupingMember]
		where [QuarterId] = @QuarterId

	delete from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost]
		where [QuarterId] = @QuarterId

	delete from [BPG_FinOps_Invoice_Reimbursement].[ProjectListing]
		where [QuarterId] = @QuarterId

	commit transaction
END
