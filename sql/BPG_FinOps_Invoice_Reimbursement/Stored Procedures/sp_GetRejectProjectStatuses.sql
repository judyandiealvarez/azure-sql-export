

-- =============================================
-- Author:      Andrii Pylypenko
-- Create Date: 2025-02-19
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetRejectProjectStatuses]
AS
BEGIN
    SET NOCOUNT ON

    select
        Name
    from [BPG_FinOps_Invoice_Reimbursement].v_ProjStatus
	where [Id] in (0, 3)
END
