create view [BPG_FinOps_Invoice_Reimbursement].[ev_StateSummary]
as
select 
    m.Id,
	s.Name SendingStatus,
	a.Name ApproveStatus,
	t.Name SubmitStatus
from [BPG_FinOps_Invoice_Reimbursement].[StateSummary] m
join [BPG_FinOps_Invoice_Reimbursement].[StateSend] s on s.Id = m.[SendingStateRefGuid]
join [BPG_FinOps_Invoice_Reimbursement].[StateApprove] a on a.Id = m.[ApproveStateRefGuid]
join [BPG_FinOps_Invoice_Reimbursement].[StateSubmit] t on t.Id = m.[SubmitStateRefGuid]
















































