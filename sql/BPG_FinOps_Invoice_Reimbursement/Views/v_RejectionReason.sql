
CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_RejectionReason]
as
select 
	s.StatusValue,
	s.Value,
	s.TagColor,
	s.TagBorderColor
from [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] s
where s.StatusName = 'Rejection Reason'

