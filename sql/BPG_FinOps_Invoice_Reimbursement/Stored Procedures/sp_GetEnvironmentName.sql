-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetEnvironmentName]
(
	@Value NVARCHAR(50) OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON

    set @Value = ISNULL((SELECT TOP 1 [Value] FROM [BPG_FinOps_Invoice_Reimbursement].[BPGParameter] 
	where [Parameter] = 'Environment'), 'Development') 
END
