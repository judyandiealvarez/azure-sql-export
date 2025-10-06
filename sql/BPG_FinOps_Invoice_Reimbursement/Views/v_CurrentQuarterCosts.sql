create view [BPG_FinOps_Invoice_Reimbursement].[v_CurrentQuarterCosts]
as
select * from [BPG_FinOps_Invoice_Reimbursement].[v_AllCosts] c
where c.QuarterStartDate <= GETDATE() and GETDATE() < c.QuarterEndDate