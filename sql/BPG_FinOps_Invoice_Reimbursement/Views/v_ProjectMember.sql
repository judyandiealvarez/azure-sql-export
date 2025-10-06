
CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_ProjectMember]
as
select
	g.QuarterId,
    g.ProjectId,
	m.Email
from [BPG_FinOps_Invoice_Reimbursement].[ProjectGrouping] g
join [BPG_FinOps_Invoice_Reimbursement].[GroupingMember] m on m.GroupingCode = g.GroupingCode and m.QuarterId = g.QuarterId
