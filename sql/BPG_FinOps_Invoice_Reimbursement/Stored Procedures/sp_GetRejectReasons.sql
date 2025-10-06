
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetRejectReasons]
AS
BEGIN
    SET NOCOUNT ON

    select * from [BPG_FinOps_Invoice_Reimbursement].[v_RejectionReason]
END
