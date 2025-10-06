


CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_Projects]
as
SELECT 
	c.ProjectId, 
	c.QuarterId,
	c.ProjectName, 
	c.Entity, 
	p.Fund,
	p.Region_Sector, 
	p.FundOrInvestment, 
	sum(c.Amount) TotalAmount,
	cast(
	case when (select top 1 Id from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc 
	where cc.IsSubmitted = 0 and cc.QuarterId = c.QuarterId and cc.ProjectId = c.ProjectId) is null then 1
	else 0 end 
	as bit) IsSubmitted
FROM BPG_FinOps_Invoice_Reimbursement.TransactionCost c
INNER JOIN BPG_FinOps_Invoice_Reimbursement.ProjectListing p ON c.ProjectId = p.ProjectId AND c.QuarterId = p.QuarterId 
GROUP BY c.ProjectId, 
	c.QuarterId,
	c.ProjectName, 
	c.Entity, 
	p.Fund,
	p.Region_Sector, 
	p.FundOrInvestment
