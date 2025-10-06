


CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_ProposalStatus]
as
SELECT 
    c.ProposalId,
	c.QuarterId,
	c.Entity,

	case
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.SentForApproval = 3
		) 
	) then 3
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.SentForApproval = 0
		) 
	) then 0
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.SentForApproval = 1
		) 
	) then 1
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.SentForApproval = 1
		) > 0
		and
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.SentForApproval = 0
		) > 0
	) then 4
	else 
	2
	end 
	SendingStatus,

	case
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.ApprovalStatus = 3
		) 
	) then 3
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.ApprovalStatus = 0
		) 
	) then 0
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.ApprovalStatus = 1
		) 
	) then 1
	else 
	2
	end 
	ApprovalStatus,

	case
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProposalId = c.ProposalId and cc.QuarterId = c.QuarterId and cc.IsSubmitted = 1
		) 
	) then 1
	else
	0
	end
	Submitted

FROM BPG_FinOps_Invoice_Reimbursement.v_Invoices c
