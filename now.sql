CREATE DATABASE azs_test;
GO
USE azs_test;
GO
CREATE SCHEMA BPG_FinOps_Invoice_Reimbursement;
GO
CREATE TABLE BPG_FinOps_Invoice_Reimbursement.TransactionCost (
  ProjectId int,
  QuarterId uniqueidentifier,
  ProposalId int,
  GroupingCode nvarchar(50)
);
CREATE TABLE BPG_FinOps_Invoice_Reimbursement.ProjectGrouping (
  ProjectId int,
  QuarterId uniqueidentifier
);
GO
IF OBJECT_ID('BPG_FinOps_Invoice_Reimbursement.c_MultigroupProposalInvoices','V') IS NOT NULL
  DROP VIEW BPG_FinOps_Invoice_Reimbursement.c_MultigroupProposalInvoices;
GO
CREATE VIEW BPG_FinOps_Invoice_Reimbursement.c_MultigroupProposalInvoices AS
SELECT TOP 1 * FROM BPG_FinOps_Invoice_Reimbursement.TransactionCost;
GO

