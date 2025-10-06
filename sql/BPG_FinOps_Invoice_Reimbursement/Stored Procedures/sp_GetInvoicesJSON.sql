
-- =============================================
-- Author:      Andrii Pylypenko
-- Create Date: 2025-02-18
-- Description: Returns cost tranactions
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetInvoicesJSON]
(
    @json nvarchar(MAX),
	@total money output,
	@pending money output,
	@inreview money output,	
	@notsent money output,	
	@approved money output,	
	@partiallyapproved money output,	
	@rejected money output
)
AS
BEGIN
    SET NOCOUNT ON

--{
--    "ContactEmail": "andrii.pylypenko@brookfieldproperties.com",
--    "Quarter": 2025, Q1,
--    "SortField": "Id",
--    "SortDirection": "Ascending",
--    "BillingMode": ""
--}

    DECLARE	@ContactEmail nvarchar(250) = JSON_VALUE(@json, '$.ContactEmail')
	DECLARE	@Quarter nvarchar(50) = JSON_VALUE(@json, '$.Quarter')
	DECLARE	@QuarterId uniqueidentifier = JSON_VALUE(@json, '$.QuarterId')
	DECLARE	@SortField nvarchar(150) = JSON_VALUE(@json, '$.SortField')
	DECLARE	@SortDirection nvarchar(50) = JSON_VALUE(@json, '$.SortDirection')
	DECLARE	@SearchText nvarchar(250) = JSON_VALUE(@json, '$.SearchText')
	DECLARE	@BillingMode integer = JSON_VALUE(@json, '$.BillingMode')

	declare @FinOps bit
	set @FinOps = isnull((select top 1 IsFinOps from [BPG_FinOps_Invoice_Reimbursement].[BillingContactRole] where [BillingContactEmail] = @ContactEmail), 0)

 --   IF @ContactEmail IS NULL or @ContactEmail = ''
	--BEGIN
	--  set @FinOps = 1
	--END

	--declare @apath nvarchar(200)
	--set @apath = (select top 1 [Value] from [BPG_FinOps_Invoice_Reimbursement].[BPGParameter] where [Parameter] = 'AttachmentsPath')

	SELECT 
		i.ProposalId, i.InvoiceDate, i.Entity, i.HeaderText, i.TotalAmount

		,ss.SummaryValue SendForApproval
		,ss.TagColor SendForApprovalTagColor
		,ss.TagBorderColor SendForApprovalTagBorderColor	

		,aps.SummaryValue ApprovalStatus
		,aps.TagColor ApprovalStatusTagColor
		,aps.TagBorderColor ApprovalStatusTagBorderColor

		,ss.Value SendForApprovalCode
		,aps.Value ApprovalStatusCode

		,q.[AttachmentsPath] + i.ProposalId + '-' + 
		(
			select top 1 l.Fund from [BPG_FinOps_Invoice_Reimbursement].[ProjectListing] l
			where l.ProjectId = 
			(
				select top 1 ProjectId from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] c 
				where c.ProposalId = i.ProposalId
			)
		) + '.zip' AttachmentsUrl
		,i.IsSubmitted
		
--	INTO #Projects
	FROM [BPG_FinOps_Invoice_Reimbursement].[v_Invoices] i
	INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[v_ProposalStatus] s on s.ProposalId = i.ProposalId and s.QuarterId = i.QuarterId and s.Entity = i.Entity
    INNER JOIN BPG_FinOps_Invoice_Reimbursement.Quarter q ON q.QuarterId = i.QuarterId
	INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] ss on ss.Value = s.SendingStatus and ss.StatusName = 'Sending Status'
    INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] aps on aps.Value = (
			case when @BillingMode = 1 and s.ApprovalStatus = 2 then 3
			else s.ApprovalStatus end
		) and aps.StatusName = 'Approval Status'

	WHERE ((q.QuarterId = @QuarterId and @QuarterId is not null) or (q.Name = @Quarter and @Quarter is not null))
	and 
	(
		(
			(@FinOps = 1)
			and
			(
			    select top 1 Id from [BPG_FinOps_Invoice_Reimbursement].[v_SecurityModel] m
				where m.[Role] = 'FinOps'
				and m.SendingStatus = s.SendingStatus
				and m.ApprovalStatus = s.ApprovalStatus
				and m.Submitted = s.Submitted
				and m.Permission = 'View'
				and m.[View] = 'Cost'
				and ((m.Milestone = 'Draft' and @BillingMode = 0) or (m.Milestone = 'Final' and @BillingMode = 1))
			) is not null
		)
		or
		(
			@ContactEmail in 
			(
				    select [Email] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectMember] m
				    INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[v_ProposalProjects] pp ON pp.projectId = m.ProjectId and pp.QuarterId = m.QuarterId
					where pp.ProposalId = i.ProposalId and pp.QuarterId = i.QuarterId
			)
			and
			(
			    select top 1 Id from [BPG_FinOps_Invoice_Reimbursement].[v_SecurityModel] m
				where m.[Role] = 'Approver'
				and m.SendingStatus = s.SendingStatus
				and m.ApprovalStatus = s.ApprovalStatus
				and m.Submitted = s.Submitted
				and m.Permission = 'View'
				and m.[View] = 'Cost'
				and ((m.Milestone = 'Draft' and @BillingMode = 0) or (m.Milestone = 'Final' and @BillingMode = 1))
			) is not null
		)
	)
	--and ((i.IsSubmitted = 1 and @BillingMode = 1) or (@BillingMode = 0) or (@BillingMode is NULL))
	
	--select * from #Projects
	
	--set @total = (select sum(TotalAmount) from #Projects)
	--set @pending = (select sum(TotalAmount) from #Projects where SentForApprovalCode = 0)
	--set @inreview = (select sum(TotalAmount) from #Projects where SentForApprovalCode in (0, 4))
	--set @notsent = (select sum(TotalAmount) from #Projects where SentForApprovalCode = 1)
	--set @approved = (select sum(TotalAmount) from #Projects where ApprovalStatusCode = 3)
	--set @rejected = (select sum(TotalAmount) from #Projects where ApprovalStatusCode = 1)
	--set @partiallyapproved = (select sum(TotalAmount) from #Projects where ApprovalStatusCode = 2)
END
