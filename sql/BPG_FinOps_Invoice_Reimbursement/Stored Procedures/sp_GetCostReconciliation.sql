-- =============================================
-- Author:      Andrii Kachanivskyi
-- Create Date: 2025-03-24
-- Description: Returns compared cost transactions
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetCostReconciliation]
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
	
    SELECT       
		c.*,

		ISNULL(rc.Amount,0) AS AmountInCostReconciliation,

		CASE 
			WHEN rc.TransactionId IS NOT NULL AND c.TransactionId IS NULL THEN 'New'
			WHEN rc.TransactionId IS NULL AND c.TransactionId IS NOT NULL THEN 'Removed'
			WHEN rc.Amount IS NOT NULL AND c.Amount <> rc.Amount THEN 'Mismatch'
			ELSE 'Match'
    	END AS CostReconciliationStatus

		FROM [BPG_FinOps_Invoice_Reimbursement].[v_AllCosts] c
		LEFT JOIN [BPG_FinOps_Invoice_Reimbursement].[Reconciliation] rc ON c.TransactionId = rc.TransactionId

		WHERE c.QuarterName = @Quarter
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
			or (c.QuarterName like '%' + @SearchText + '%')
			or (c.RejectionReason like '%' + @SearchText + '%')
			or (c.Comments like '%' + @SearchText + '%')
			or (c.Contacts like '%' + @SearchText + '%')
			or (c.ProjectStatus like '%' + @SearchText + '%')
			or (c.Fund like '%' + @SearchText + '%')
			or (c.HeaderText like '%' + @SearchText + '%')
		)
		and ((c.IsSubmitted = 1 and @BillingMode = 1) or (@BillingMode = 0) or (@BillingMode is NULL))


END
