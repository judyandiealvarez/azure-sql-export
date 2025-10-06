
CREATE view [BPG_FinOps_Invoice_Reimbursement].[ev_SecurityCostStatePermission]
as
select 
    sp.Id,
    r.Name [Role],
	s.SendingStatus,
	s.ApprovalStatus,
	s.Submitted,
	v.Name [View],
	p.Name [Permission],
	m.Name Milestone
from [BPG_FinOps_Invoice_Reimbursement].[SecurityCostStatePermission] sp 
join [BPG_FinOps_Invoice_Reimbursement].[SecurityCostState] s on s.Id = sp.StateRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityPermission] p on p.Id = sp.PermissionRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityView] v on v.Id = sp.ViewRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityMilestone] m on m.Id = sp.MilestoneRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityRole] r on r.Id = sp.RoleRefGuid






