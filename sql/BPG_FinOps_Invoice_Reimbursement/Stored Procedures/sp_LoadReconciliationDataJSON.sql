-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_LoadReconciliationDataJSON]
(
@JsonData NVARCHAR(MAX)
)
AS
BEGIN
    SET NOCOUNT ON;
     INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[Reconciliation] (
        Id, QuarterId, TransactionId, ProposalId, InvoiceDate, HeaderText, 
         AccountingDate, DocumentDate, CutomerAccount, ProjectId, 
        ProjectName, VendorAccount, VendorName, InvoiceNo, Description, 
        CategoryCode, CategoryName, Amount, Currency
        -- CreatedOn, ModifiedOn, 
        -- Quarter, IsSubmitted, SubmissionDate, SentForApproval, SentForApprovalDate, 
        -- ApprovalStatus, RejectionReason, Comments, DateApproved, Attachments, 
        -- Contacts, IsAutoApproved, CorrectReason
    )
    SELECT
        newid(),
        newid(),
        jsonData.TransactionId,
        jsonData.ProposalId,
        jsonData.InvoiceDate,
        jsonData.HeaderText,
        --jsonData.Entity,
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
        jsonData.Currency
        -- jsonData.Quarter,
        -- jsonData.IsSubmitted,
        -- jsonData.SubmissionDate,
        -- jsonData.SentForApproval,
        -- jsonData.SentForApprovalDate,
        -- jsonData.ApprovalStatus,
        -- jsonData.RejectionReason,
        -- jsonData.Comments,
        -- jsonData.DateApproved,
        -- jsonData.Attachments,
        -- jsonData.Contacts,
        -- jsonData.IsAutoApproved,
        -- jsonData.CorrectReason
    FROM OPENJSON(@JsonData)
    WITH(
        -- Id UNIQUEIDENTIFIER '$.Id',
        -- QuarterId UNIQUEIDENTIFIER '$.QuarterId',
        TransactionId NVARCHAR(25) '$.prox_transactionid',
        ProposalId NVARCHAR(25) '$.prox_proposalid',
        InvoiceDate DATE '$.prox_invoicedate',
        HeaderText NVARCHAR(100) '$.prox_headertext',
        --Entity NVARCHAR(10) '$.Entity',
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
        Currency NVARCHAR(3) '$.prox_costcurrency'
        --CreatedOn DATETIME '$.CreatedOn',
        --ModifiedOn DATETIME '$.ModifiedOn',
        --Quarter NVARCHAR(10) '$.Quarter',
        --IsSubmitted BIT '$.IsSubmitted',
        --SubmissionDate DATETIME '$.SubmissionDate',
       --SentForApproval INT '$.SentForApproval',
        --SentForApprovalDate DATETIME '$.SentForApprovalDate',
        --ApprovalStatus INT '$.ApprovalStatus',
        --RejectionReason NVARCHAR(50) '$.RejectionReason',
        --Comments NVARCHAR(200) '$.Comments',
        --DateApproved DATE '$.DateApproved',
        --Attachments NVARCHAR(300) '$.Attachments',
        --Contacts NVARCHAR(300) '$.Contacts',
        --IsAutoApproved BIT '$.IsAutoApproved',
        --CorrectReason NVARCHAR(200) '$.CorrectReason'
    ) AS jsonData;
--,prox_balance,,,,,,,,,,,prox_projectcontractid,prox_projectgroup,,,,prox_salescurrency,,prox_totalsalesamount,,,,prox_voucher

END

