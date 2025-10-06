
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadPPTDataJSON]
(
	@ImportId uniqueidentifier,
    @JsonData NVARCHAR(MAX)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;
     INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[PPT] (
ImportId,
Transaction_ID,
Proposal_Id,
Invoice_date,
Header_text,
Accounting_Date,
Document_Date,
Customer_Account,
Project_Id,
Project_name,
Vendor_Account,
Vendor_Name,
Invoice_No,
Description,
Category_Code,
Category_name,
Total_Cost_Amount,
Cost_currency,
Total_sales_amount,
Sales_currency,
Voucher,
Project_group,
Balance,
Project_Contract_Id

    )
    SELECT
        @ImportId,
        jsonData.TransactionId,
        jsonData.ProposalId,
        jsonData.InvoiceDate,
        jsonData.HeaderText,
        jsonData.AccountingDate,
        jsonData.DocumentDate,
        jsonData.CutomerAccount,
        jsonData.ProjectId,
        jsonData.ProjectName,
        jsonData.VendorAccount,
        jsonData.VendorName,
        jsonData.InvoiceNo,
        jsonData.Description,
        jsonData.CategoryCode,
        jsonData.CategoryName,
        jsonData.Amount,
        jsonData.Currency,
        jsonData.TotalSalesAmout,
        jsonData.SalesCurrency,
        jsonData.Voucher,
        jsonData.ProjectGroup,
        jsonData.Balance,
        jsonData.ProjectContractId
    FROM OPENJSON(@JsonData)
    WITH(
        TransactionId NVARCHAR(25) '$.prox_transactionid',
        ProposalId NVARCHAR(25) '$.prox_proposalid',
        InvoiceDate DATE '$.prox_invoicedate',
        HeaderText NVARCHAR(100) '$.prox_headertext',
        AccountingDate DATE '$.prox_accountingdate',
        DocumentDate DATE '$.prox_documentdate',
        CutomerAccount NVARCHAR(100) '$.prox_customeraccount',
        ProjectId NVARCHAR(50) '$.prox_projectid',
        ProjectName NVARCHAR(100) '$.prox_projectname',
        VendorAccount NVARCHAR(25) '$.prox_vendoraccount',
        VendorName NVARCHAR(100) '$.prox_vendorname',
        InvoiceNo NVARCHAR(25) '$.prox_invoiceno',
        Description NVARCHAR(200) '$.prox_description',
        CategoryCode NVARCHAR(25) '$.prox_categorycode',
        CategoryName NVARCHAR(100) '$.prox_categoryname',
        Amount MONEY '$.prox_totalcostamount',
        Currency NVARCHAR(3) '$.prox_costcurrency',
        TotalSalesAmout MONEY '$.prox_totalsalesamount',
        SalesCurrency NVARCHAR(3) '$.prox_salescurrency',
        Voucher NVARCHAR(50) '$.prox_voucher',
        ProjectGroup NVARCHAR(50) '$.prox_projectgroup',
        Balance MONEY '$.prox_balance',
        ProjectContractId NVARCHAR(50) '$.prox_projectcontractid'

    ) AS jsonData;

END
