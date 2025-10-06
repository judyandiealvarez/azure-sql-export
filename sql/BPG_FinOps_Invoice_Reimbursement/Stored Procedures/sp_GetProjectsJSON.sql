
-- =============================================
-- Author:      Andrii Pylypenko
-- Create Date: 2025-02-18
-- Description: Returns projects
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetProjectsJSON]
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
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

--{
--    "ContactEmail": "andrii.pylypenko@brookfieldproperties.com",
--    "Quarter": "2025, Q1",
--    "QuarterId": "some guid",
--    "SortField": "Id",
--    "SortDirection": "Ascending",
--    "SearchText": "",
--    "BillingMode": ""
--}

    DECLARE	@ContactEmail nvarchar(250) = JSON_VALUE(@json, '$.ContactEmail')
	DECLARE	@Quarter nvarchar(50) = JSON_VALUE(@json, '$.Quarter')
	DECLARE	@QuarterId uniqueidentifier = JSON_VALUE(@json, '$.QuarterId')
	DECLARE	@SortField nvarchar(150) = JSON_VALUE(@json, '$.SortField')
	DECLARE	@SortDirection nvarchar(50) = JSON_VALUE(@json, '$.SortDirection')
	DECLARE	@SearchText nvarchar(250) = JSON_VALUE(@json, '$.SearchText')
	DECLARE	@BillingMode integer = JSON_VALUE(@json, '$.BillingMode')

	INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[Log]
           ([Source]
           ,[Description])
     VALUES
     (
		'[BPG_FinOps_Invoice_Reimbursement].[sp_GetProjects]',
		cast('Begin args: @ContactEmail = ''' + isnull(@ContactEmail, 'NULL') + 
		''' @Quarter = ''' + isnull(@Quarter, 'NULL') + 
		''' @BillingMode = ''' + convert(nvarchar(100), isnull(@BillingMode, 0)) + 
		'''' as nvarchar(200))
	 )

	declare @FinOps bit
	set @FinOps = isnull((select top 1 IsFinOps from [BPG_FinOps_Invoice_Reimbursement].[BillingContactRole] where [BillingContactEmail] = @ContactEmail), 0)

 --   IF @ContactEmail IS NULL or @ContactEmail = ''
	--BEGIN
	--  set @FinOps = 1
	--END

	SELECT 
	    p.ProjectId, 
	    p.ProjectName, 
		p.Entity, 
		p.Fund,
		p.Region_Sector, 
		p.FundOrInvestment, 
		p.TotalAmount

		,ss.SummaryValue SendForApproval
		,ss.TagColor SendForApprovalTagColor
		,ss.TagBorderColor SendForApprovalTagBorderColor	

		,aps.SummaryValue ApprovalStatus
		,aps.TagColor ApprovalStatusTagColor
		,aps.TagBorderColor ApprovalStatusTagBorderColor

		,ps.SummaryValue ProjectStatus
		,ps.TagColor ProjectStatusTagColor
		,ps.TagBorderColor ProjectStatusTagBorderColor

		,ss.Value SendForApprovalCode
		,aps.Value ApprovalStatusCode
		
		, isnull((select top 1 pg.[grouping] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectGrouping] pg where pg.project_code = p.ProjectId), '') GroupingCode
		,p.IsSubmitted
		
--	INTO #Projects
	FROM [BPG_FinOps_Invoice_Reimbursement].[v_Projects] p
	INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[v_ProjectStatus] s on s.ProjectId = p.ProjectId and s.QuarterId = p.QuarterId and s.Entity = p.Entity
    INNER JOIN BPG_FinOps_Invoice_Reimbursement.Quarter q ON q.QuarterId = p.QuarterId
	
		INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] ss on ss.Value = s.SendingStatus and ss.StatusName = 'Sending Status'
        INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] aps on aps.Value = (
			case when @BillingMode = 1 and s.ApprovalStatus = 2 then 3
			else s.ApprovalStatus end
		) and aps.StatusName = 'Approval Status'
		INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] ps on ps.Value = s.ProjectStatus and ps.StatusName = 'Project Status' 

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
				and m.[View] = 'Project Summary'
				and ((m.Milestone = 'Draft' and (@BillingMode = 0 or @BillingMode is null)) or (m.Milestone = 'Final' and @BillingMode = 1))
			) is not null
		)
		or
		(
			@ContactEmail in 
			(
				    select [Email] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectMember] m
					where m.ProjectId = p.ProjectId and m.QuarterId = p.QuarterId
			)
			and
			(
			    select top 1 Id from [BPG_FinOps_Invoice_Reimbursement].[v_SecurityModel] m
				where m.[Role] = 'Approver'
				and m.SendingStatus = s.SendingStatus
				and m.ApprovalStatus = s.ApprovalStatus
				and m.Submitted = s.Submitted
				and m.Permission = 'View'
				and m.[View] = 'Project Summary'
				and ((m.Milestone = 'Draft' and (@BillingMode = 0 or @BillingMode is null)) or (m.Milestone = 'Final' and @BillingMode = 1))
			) is not null
		)
	)
	--and ((p.IsSubmitted = 1 and @BillingMode = 1) or (@BillingMode = 0) or (@BillingMode is NULL))

	--select * from #Projects
	
	--set @total = (select sum(TotalAmount) from #Projects)
	--set @pending = (select sum(TotalAmount) from #Projects where SentForApprovalCode = 0)
	--set @inreview = (select sum(TotalAmount) from #Projects where SentForApprovalCode in (0, 4))
	--set @notsent = (select sum(TotalAmount) from #Projects where SentForApprovalCode = 1)
	--set @approved = (select sum(TotalAmount) from #Projects where ApprovalStatusCode = 3)
	--set @rejected = (select sum(TotalAmount) from #Projects where ApprovalStatusCode = 1)
	--set @partiallyapproved = (select sum(TotalAmount) from #Projects where ApprovalStatusCode = 2)
END
