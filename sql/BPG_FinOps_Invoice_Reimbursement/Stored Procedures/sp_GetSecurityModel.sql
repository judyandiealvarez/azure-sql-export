-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetSecurityModel]
AS
BEGIN
    SET NOCOUNT ON

    select 
	    r.Name as [Role],
		(
		    select * from (
				select 
					s.SendingStatus,
					s.ApprovalStatus,
					s.Submitted,
					v.Name [View],
					p.Name [Permission],
					m.Name Milestone
				from [BPG_FinOps_Invoice_Reimbursement].[SecurityCostStatePermission] sp 
				join [BPG_FinOps_Invoice_Reimbursement].[SecurityPermission] p on p.Id = sp.PermissionRefGuid
				join [BPG_FinOps_Invoice_Reimbursement].[SecurityCostState] s on s.Id = sp.StateRefGuid
				join [BPG_FinOps_Invoice_Reimbursement].[SecurityView] v on v.Id = sp.ViewRefGuid
				join [BPG_FinOps_Invoice_Reimbursement].[SecurityMilestone] m on m.Id = sp.MilestoneRefGuid
				where sp.RoleRefGuid = r.Id
			) i
			for json auto

		) [CostStatePermissions]
	from [BPG_FinOps_Invoice_Reimbursement].[SecurityRole] r	
	for json auto
END
