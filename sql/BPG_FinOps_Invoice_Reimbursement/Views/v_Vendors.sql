
CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_Vendors]
as
select distinct VendorAccount, VendorName from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost]
