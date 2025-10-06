-- =============================================
-- Author:      Andrii Kachanivskyi
-- Create Date: 2025-03-24
-- Description: Return compared invoices
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GetInvoicesReconciliation]
(
    @ContactEmail nvarchar(250),
	@Quarter nvarchar(50) = NULL,
	@BillingMode integer = NULL
)
AS
BEGIN
    SET NOCOUNT ON

    declare @FinOps bit
	set @FinOps = isnull((select top 1 IsFinOps from [BPG_FinOps_Invoice_Reimbursement].[BillingContactRole] where [BillingContactEmail] = @ContactEmail), 0)

    SELECT 
i.ProposalId, i.InvoiceDate, i.Entity, i.HeaderText, i.TotalAmount

        ,COALESCE(rc.TotalAmountD365,0) AS TotalAmountD365

		,ss.StatusValue SendForApproval
		,ss.TagColor SendForApprovalTagColor
		,ss.TagBorderColor SendForApprovalTagBorderColor	

		,aps.StatusValue ApprovalStatus
		,aps.TagColor ApprovalStatusTagColor
		,aps.TagBorderColor ApprovalStatusTagBorderColor

		,ss.Value SendForApprovalCode
		,aps.Value ApprovalStatusCode

,
CASE 
        WHEN ISNULL(i.TotalAmount, 0) <> ISNULL(rc.TotalAmountD365, 0) THEN 'Mismatch'
        ELSE 'Match'
    END AS ReconciliationStatus
FROM [BPG_FinOps_Invoice_Reimbursement].[v_Invoices] i
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[v_ProposalStatus] s on s.ProposalId = i.ProposalId and s.QuarterId = i.QuarterId and s.Entity = i.Entity
INNER JOIN BPG_FinOps_Invoice_Reimbursement.Quarter q ON q.QuarterId = i.QuarterId
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] ss on ss.Value = s.SendingStatus and ss.StatusName = 'Sending Status'
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] aps on aps.Value = (
			case when @BillingMode = 1 and s.ApprovalStatus = 2 then 3
			else s.ApprovalStatus end
		) and aps.StatusName = 'Approval Status'
LEFT JOIN (SELECT 
        ProposalId, 
        SUM(Amount) AS TotalAmountD365
    FROM [BPG_FinOps_Invoice_Reimbursement].[Reconciliation]
    GROUP BY ProposalId ) rc ON i.ProposalId = rc.ProposalId


        WHERE q.Name = @Quarter and 
	(
		(@FinOps = 1)
		or
		(
			@ContactEmail in 
			(
				    select [Email] from [BPG_FinOps_Invoice_Reimbursement].[v_ProjectMember] m
				    INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[v_ProposalProjects] pp ON pp.projectId = m.ProjectId and pp.QuarterId = m.QuarterId
					where pp.ProposalId = i.ProposalId and pp.QuarterId = i.QuarterId
			)
		)
	)
	and ((i.IsSubmitted = 1 and @BillingMode = 1) or (@BillingMode = 0) or (@BillingMode is NULL))


END
