


CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_Invoices]
as
SELECT 
    c.ProposalId, c.QuarterId, c.InvoiceDate, c.Entity, c.HeaderText, sum(c.Amount) TotalAmount,
	cast(case when (select top 1 Id from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc 
	where cc.IsSubmitted = 0 and cc.QuarterId = c.QuarterId and cc.ProposalId = c.ProposalId) is null then 1
	else 0 end
	as bit) IsSubmitted
FROM BPG_FinOps_Invoice_Reimbursement.TransactionCost c
INNER JOIN BPG_FinOps_Invoice_Reimbursement.ProjectListing p ON c.ProjectId = p.ProjectId AND c.QuarterId = p.QuarterId 
GROUP BY c.ProposalId, c.QuarterId, c.InvoiceDate, c.Entity, c.HeaderText
