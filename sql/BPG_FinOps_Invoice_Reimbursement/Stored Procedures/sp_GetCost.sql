

-- =============================================
-- Author:      Andrii Pylypenko
-- Create Date: 2025-02-18
-- Description: Returns cost tranactions
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetCost]
(
    @ContactEmail nvarchar(250),
	@Quarter nvarchar(50) = NULL,
	@SortField nvarchar(150) = NULL,
	@SortDirection nvarchar(50) = NULL,
	@SearchText nvarchar(250) = NULL,
	@ProposalId nvarchar(50) = NULL,
	@ProjectId nvarchar(50) = NULL,
	@BillingMode integer = NULL
)
AS
BEGIN
    SET NOCOUNT ON

    INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[Log]
           ([Source]
           ,[Description])
     VALUES
           ('[BPG_FinOps_Invoice_Reimbursement].[sp_GetCost]'
           ,'Begin args: @ContactEmail = ''' 
		    + @ContactEmail + ''' @Quarter = '''
		    + isnull(@Quarter, 'NULL') + ''' @SortField = '''
			+ isnull(@SortField, 'NULL') + ''' @SortDirection = '''
			+ isnull(@SortDirection, 'NULL') + ''' @SearchText = '''
			+ isnull(@SearchText, 'NULL') + ''' @ProposalId = '''
			+ isnull(@ProposalId, 'NULL') + ''' @ProjectId = '''
			+ isnull(@ProjectId, 'NULL') + '''')

	declare @FinOps bit
	set @FinOps = isnull(
		(select top 1 IsFinOps from [BPG_FinOps_Invoice_Reimbursement].[BillingContactRole] 
		 where [BillingContactEmail] = @ContactEmail), 
		 0
	)

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
		q.[AttachmentsPath] + c.ProposalId + '-' + 
		(
			select top 1 l.Fund from [BPG_FinOps_Invoice_Reimbursement].[ProjectListing] l
			where l.ProjectId = c.ProjectId
		) + '.zip' AttachmentsUrl,
		c.LocalCurrency,
		c.LocalCurrencyAmount,
		c.JournalAction,
		c.JournalDescriptionComment,
		c.JournalDescription
	FROM BPG_FinOps_Invoice_Reimbursement.TransactionCost c
	INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus]		ss	on ss.Value			= c.SentForApproval and ss.StatusName	= 'Sending Status'
    INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus]		aps on aps.Value		= c.ApprovalStatus	and aps.StatusName	= 'Approval Status'
	INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[ProjectListing]	p	ON c.ProjectId		= p.ProjectId		AND c.QuarterId		= p.QuarterId 
	INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus]		ps	on ps.StatusName	= 'Project Status'	and ps.Value		= p.ProjectStatus
	INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[Quarter]			q	ON c.QuarterId		= q.QuarterId
	WHERE (
		(@FinOps = 1)
		or
		(
			@ContactEmail in 
			(
					select [Email] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectMember] m
					where m.ProjectId = p.ProjectId and m.QuarterId = p.QuarterId
			)
		)
	)
	AND q.Name = @Quarter
	and (@ProposalId is NULL or (c.ProposalId = @ProposalId and @ProposalId is not NULL))
	and (@ProjectId is NULL or (c.ProjectId = @ProjectId and @ProjectId is not NULL))
	and (@SearchText is NULL 
		or (c.TransactionId like '%' + @SearchText + '%')
		or (c.ProposalId like '%' + @SearchText + '%')
		or (c.Entity like '%' + @SearchText + '%')
		or (c.CutomerAccount like '%' + @SearchText + '%')
		or (c.ProjectId like '%' + @SearchText + '%')
		or (c.ProjectName like '%' + @SearchText + '%')
		or (c.VendorAccount like '%' + @SearchText + '%')
		or (c.VendorName like '%' + @SearchText + '%')
		or (c.InvoiceNo like '%' + @SearchText + '%')
		or (c.Description like '%' + @SearchText + '%')
		or (c.CategoryCode like '%' + @SearchText + '%')
		or (c.CategoryName like '%' + @SearchText + '%')
		or (c.Currency like '%' + @SearchText + '%')
		or (q.Name like '%' + @SearchText + '%')
		or (c.RejectionReason like '%' + @SearchText + '%')
		or (c.Comments like '%' + @SearchText + '%')
		or (c.Contacts like '%' + @SearchText + '%')
		or (p.ProjectStatus like '%' + @SearchText + '%')
		or (p.Fund like '%' + @SearchText + '%')
		or (c.HeaderText like '%' + @SearchText + '%')
	)
	and ((c.IsSubmitted = 1 and @BillingMode = 1) or (c.IsSubmitted = 0 and (@BillingMode = 0 or @BillingMode is NULL)))
END
