
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadQuarterMPLData]
(
	@ImportId uniqueidentifier
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

	declare @QuarterId uniqueidentifier

	set @QuarterId = (select top 1 QuarterId from [BPG_FinOps_Invoice_Reimbursement].[ImportSession] where [ImportId] = @ImportId)

	INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[ProjectListing]
           ([ProjectId]
           ,[ProjectStatus]
           ,[Fund]
           ,[QuarterId]
           ,[Region_Sector]
           ,[FundOrInvestment])
	select distinct
	  c.project_code,
	  s.Value,
	  c.fund,
	  @QuarterId,
	  c.sector_region,
	  c.fund_or_investment
	from [BPG_Fin_Ops].[v_Project_Invoice_Contacts] c
	join [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] s on s.StatusName = 'Project Status' and s.StatusValue = c.project_status
	where c.project_code not in 
	  (
	    select 
	      project_code 
	    from [BPG_FinOps_Invoice_Reimbursement].[ProjectListing] 
	    where QuarterId = @QuarterId
      )

END
