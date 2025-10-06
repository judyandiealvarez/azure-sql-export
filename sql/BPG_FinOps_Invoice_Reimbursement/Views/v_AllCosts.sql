

CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_AllCosts]
as
SELECT 
	c.Id,
	c.TransactionId, 
	c.ProposalId, 
	c.InvoiceDate, 
	c.Entity, 
	c.AccountingDate, 
	c.DocumentDate, 
	c.CutomerAccount, 
	c.ProjectId, 
	c.ProjectName, 
	c.VendorAccount, 
	c.VendorName, 
	c.InvoiceNo, 
	c.Description,
	c.CategoryCode, 
	c.CategoryName, 
	c.Amount, 
	c.Currency, 
	c.ModifiedOn, 
	c.Quarter, 
	c.IsSubmitted,
	c.SubmissionDate, 
	ss.StatusValue SentForApproval,
	c.SentForApprovalDate,
	aps.StatusValue ApprovalStatus, 
	c.RejectionReason, 
	c.Comments, 
	c.DateApproved, 
	c.Attachments, 
	c.QuarterId, 
	q.Name AS QuarterName, 
	c.Contacts, 
	ps.StatusValue ProjectStatus, 
	p.Fund, 
	p.Region_Sector,
	p.FundOrInvestment,
	c.HeaderText, 
	c.CreatedOn,
	ss.TagColor SendForApprovalTagColor,
	ss.TagBorderColor SendForApprovalTagBorderColor,		
	aps.TagColor ApprovalStatusTagColor,
	aps.TagBorderColor ApprovalStatusTagBorderColor,
	(select top 1 [grouping] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectGrouping] where project_code = c.ProjectId) [Grouping],		
	--ps.TagColor ProjectStatusTagColor,
	--ps.TagBorderColor ProjectStatusTagBorderColor,
	c.SentForApproval SentForApprovalCode,
	c.ApprovalStatus ApprovalStatusCode,
	p.ProjectStatus ProjectStatusCode,
	(case when c.SentForApprovalDate is null then 0 else 1 end) IsSent,
	q.StartDate QuarterStartDate,
	q.EndDate QuarterEndDate
FROM BPG_FinOps_Invoice_Reimbursement.TransactionCost c
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus]		ss	on ss.Value			= c.SentForApproval and ss.StatusName	= 'Sending Status'
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus]		aps on aps.Value		= c.ApprovalStatus	and aps.StatusName	= 'Approval Status'
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[ProjectListing]	p	ON c.ProjectId		= p.ProjectId		AND c.QuarterId		= p.QuarterId 
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus]		ps	on ps.StatusName	= 'Project Status'	and ps.Value		= p.ProjectStatus
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[Quarter]			q	ON c.QuarterId		= q.QuarterId
