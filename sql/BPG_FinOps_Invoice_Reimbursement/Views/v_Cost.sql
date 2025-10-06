
CREATE VIEW [BPG_FinOps_Invoice_Reimbursement].[v_Cost]
AS
SELECT
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.TransactionId, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.ProposalId, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.InvoiceDate, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Entity, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.AccountingDate, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.DocumentDate, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.CutomerAccount, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.ProjectId, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.ProjectName, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.VendorAccount, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.VendorName, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.InvoiceNo, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Description, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.CategoryCode, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.CategoryName, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Amount, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Currency, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.ModifiedOn, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Quarter, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.IsSubmitted, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.SubmissionDate, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.SentForApproval, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.SentForApprovalDate, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.ApprovalStatus, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.RejectionReason, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Comments, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.DateApproved, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Attachments, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.QuarterId, 
  BPG_FinOps_Invoice_Reimbursement.Quarter.Name AS QuarterName, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.Contacts, 
  BPG_FinOps_Invoice_Reimbursement.ProjectListing.ProjectStatus, 
  BPG_FinOps_Invoice_Reimbursement.ProjectListing.Fund, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.HeaderText, 
  BPG_FinOps_Invoice_Reimbursement.TransactionCost.CreatedOn
FROM BPG_FinOps_Invoice_Reimbursement.TransactionCost 
LEFT OUTER JOIN BPG_FinOps_Invoice_Reimbursement.ProjectListing ON BPG_FinOps_Invoice_Reimbursement.TransactionCost.ProjectId = BPG_FinOps_Invoice_Reimbursement.ProjectListing.ProjectId 
INNER JOIN BPG_FinOps_Invoice_Reimbursement.Quarter ON BPG_FinOps_Invoice_Reimbursement.TransactionCost.QuarterId = BPG_FinOps_Invoice_Reimbursement.Quarter.QuarterId
