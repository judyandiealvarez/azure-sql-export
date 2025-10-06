



CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_ProjectStatus]
as
SELECT 
	c.ProjectId, 
	c.QuarterId,
	c.Entity,

	case
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.SentForApproval = 3
		) 
	) then 3
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.SentForApproval = 0
		) 
	) then 0
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.SentForApproval = 1
		) 
	) then 1
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.SentForApproval = 1
		) > 0
		and
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.SentForApproval = 0
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
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.ApprovalStatus = 3
		) 
	) then 3
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.ApprovalStatus = 0
		) 
	) then 0
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.ApprovalStatus = 1
		) 
	) then 1
	else 
	2
	end 
	ApprovalStatus,

	s.Value ProjectStatus,

	case
	when (
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity
		) 
		=
		(
		select count(Id) from BPG_FinOps_Invoice_Reimbursement.TransactionCost cc
		where cc.ProjectId = c.ProjectId and cc.QuarterId = c.QuarterId and cc.Entity = c.Entity and cc.IsSubmitted = 1
		) 
	) then 1
	else
	0
	end
	Submitted

FROM BPG_FinOps_Invoice_Reimbursement.v_Projects c
INNER JOIN BPG_FinOps_Invoice_Reimbursement.ProjectListing p ON c.ProjectId = p.ProjectId AND c.QuarterId = p.QuarterId
INNER JOIN [BPG_FinOps_Invoice_Reimbursement].[BPGStatus] s on s.StatusName = 'Project Status' and s.Value = p.ProjectStatus
