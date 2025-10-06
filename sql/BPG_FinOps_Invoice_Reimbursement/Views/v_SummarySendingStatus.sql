


create view [BPG_FinOps_Invoice_Reimbursement].[v_SummarySendingStatus]
as
select 
	s.Value Id,
	s.SummaryValue Name,
	s.TagColor,
	s.TagBorderColor
from [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] s
where s.StatusName = 'Sending Status'

