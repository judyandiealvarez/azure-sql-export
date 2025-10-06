


CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_SendingStatus]
as
select 
	s.Value Id,
	s.StatusValue Name,
	s.TagColor,
	s.TagBorderColor
from [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] s
where s.StatusName = 'Sending Status'

