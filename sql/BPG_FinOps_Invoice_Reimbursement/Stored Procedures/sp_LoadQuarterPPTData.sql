
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadQuarterPPTData]
(
	@ImportId uniqueidentifier
)
AS
BEGIN
    SET NOCOUNT ON

	declare @QuarterId uniqueidentifier
	declare @Quarter nvarchar(50)

	set @QuarterId = (select top 1 QuarterId from [BPG_FinOps_Invoice_Reimbursement].[ImportSession] where [ImportId] = @ImportId)
	set @Quarter = (select top 1 [Name] from [BPG_FinOps_Invoice_Reimbursement].[Quarter] where [QuarterId] = @QuarterId)

    INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[TransactionCost]
           ([QuarterId]
           ,[TransactionId]
           ,[ProposalId]
           ,[InvoiceDate]
           ,[HeaderText]
           ,[Entity]
           ,[AccountingDate]
           ,[DocumentDate]
           ,[CutomerAccount]
           ,[ProjectId]
           ,[ProjectName]
           ,[VendorAccount]
           ,[VendorName]
           ,[InvoiceNo]
           ,[Description]
           ,[CategoryCode]
           ,[CategoryName]
           ,[Amount]
           ,[Currency]
           ,[CreatedOn]
           ,[ModifiedOn]
           ,[Quarter]
		   
		   ,IsSubmitted
		   ,SentForApproval
		   ,ApprovalStatus
		   ,LocalCurrency
		   ,LocalCurrencyAmount
		   )

	select

	@QuarterId [QuarterId],
	ppt.Transaction_Id,
	ppt.proposal_Id,
	ppt.invoice_date,
	ppt.header_text,
	SUBSTRING(ppt.Transaction_Id, 1, 4) Entity,
	ppt.Accounting_date,
	ppt.Document_date,
	ppt.customer_account,
	SUBSTRING(ppt.[Project_Id], 1, 10) [ProjectId],
	ppt.Project_name,
	ppt.Vendor_Account,
	ppt.Vendor_Name,
	ppt.Invoice_No,
	ppt.Description,
	ppt.Category_Code,
	ppt.Category_name,
	ppt.Total_Cost_Amount Amount,
	ppt.Cost_currency,
	ppt.Invoice_date CreatedOn,
	ppt.Invoice_date ModifiedOn,
	@Quarter [Quarter],

	0,
	0,
	0,
	ppt.Cost_currency,
	ppt.Total_sales_amount

	from [BPG_FinOps_Invoice_Reimbursement].[PPT] ppt
	where ppt.ImportId = @ImportId
	and ppt.Transaction_Id not in (select TransactionId from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] where QuarterId = @QuarterId)
END
