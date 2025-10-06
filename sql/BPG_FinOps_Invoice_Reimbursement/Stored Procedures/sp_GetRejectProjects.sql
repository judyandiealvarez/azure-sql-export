
-- =============================================
-- Author:      Andrii Pylypenko
-- Create Date: 2025-02-19
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetRejectProjects]
AS
BEGIN
    SET NOCOUNT ON

    select distinct
        ProjectId Name,
		ProjectName,
		Fund
    from [BPG_FinOps_Invoice_Reimbursement].v_Projects
END
