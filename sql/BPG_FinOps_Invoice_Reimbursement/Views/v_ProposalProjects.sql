
CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_ProposalProjects]
as
select
  [QuarterId], [ProposalId], [ProjectId]
from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost]
group by [QuarterId], [ProposalId], [ProjectId]

