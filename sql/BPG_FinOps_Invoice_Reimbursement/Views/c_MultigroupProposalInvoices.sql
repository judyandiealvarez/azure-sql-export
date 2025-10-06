create view [BPG_FinOps_Invoice_Reimbursement].[c_MultigroupProposalInvoices]
as
select f.ProposalId, count(*) cnt, string_agg(f.GroupingCode, ', ') [groups] from (

select 
g.GroupingCode,
c.ProposalId,
count(distinct c.ProjectId) pcnt,
count(*) cnt
from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] c
join [BPG_FinOps_Invoice_Reimbursement].[ProjectGrouping] g on g.ProjectId = c.ProjectId and g.QuarterId = c.QuarterId
where c.QuarterId = 'A10C1605-CBD9-4360-A7AB-088F74D8C20A'
group by g.GroupingCode,
c.ProposalId
having count(distinct c.ProjectId) > 1


) f
group by f.ProposalId
having count(*) > 1