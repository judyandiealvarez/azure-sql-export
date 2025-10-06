



CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_SecurityModel]
as
select 
    sp.Id,
    r.Id RoleId,
    r.Name [Role],
	s.Id StateId,
	s.SendingStatus,
	ss.StatusValue SendingStatusName,
	s.ApprovalStatus,
	aps.StatusValue ApprovalStatusName,
	s.Submitted,
	v.Id ViewId,
	v.Name [View],
	m.Id MilestoneId,
	m.Name Milestone,
	p.Id PermissionId,
	p.Name [Permission]
from [BPG_FinOps_Invoice_Reimbursement].[SecurityCostStatePermission] sp 
join [BPG_FinOps_Invoice_Reimbursement].[SecurityRole] r on r.Id = sp.RoleRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityPermission] p on p.Id = sp.PermissionRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityCostState] s on s.Id = sp.StateRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityView] v on v.Id = sp.ViewRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[SecurityMilestone] m on m.Id = sp.MilestoneRefGuid
join [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] ss on ss.Value = s.SendingStatus and ss.StatusName = 'Sending Status'
join [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] aps on aps.Value = s.ApprovalStatus and aps.StatusName = 'Approval Status'
