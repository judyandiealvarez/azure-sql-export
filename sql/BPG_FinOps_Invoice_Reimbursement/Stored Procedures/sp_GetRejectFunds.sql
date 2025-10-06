
-- =============================================
-- Author:      Andrii Pylypenko
-- Create Date: 2025-02-19
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetRejectFunds]
AS
BEGIN
    SET NOCOUNT ON

    select distinct
        Fund Name
    from [BPG_FinOps_Invoice_Reimbursement].v_Projects
END
