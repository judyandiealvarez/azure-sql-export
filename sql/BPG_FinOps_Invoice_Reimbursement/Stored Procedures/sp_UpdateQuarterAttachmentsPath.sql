-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_UpdateQuarterAttachmentsPath]
(
    @Id uniqueidentifier,
	@AttachmentsPath nvarchar(200)
)
AS
BEGIN
    SET NOCOUNT ON

	update top(1) q
	set [AttachmentsPath] = @AttachmentsPath
	from [BPG_FinOps_Invoice_Reimbursement].[Quarter] q
	where q.QuarterId = @Id
END
