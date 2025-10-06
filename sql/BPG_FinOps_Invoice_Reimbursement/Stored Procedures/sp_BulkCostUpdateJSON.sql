
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_BulkCostUpdateJSON]
(
    @json nvarchar(MAX)
)
AS
BEGIN
    SET NOCOUNT ON

--{
--    "ApprovalDate": "2025-03-18T17:53:51.202Z",
--    "ApprovalStatus": 3,
--    "ApprovedBy": "andrii.pylypenko@brookfieldproperties.com",
--    "Comments": "",
--    "CorrectReason": "",
--    "Ids": [
--        {
--            "Id": "51798241-c9cb-4b2f-9407-250982848d58"
--        }
--    ],
--    "QuarterId": "8f25ce21-3031-46da-bffb-9c6ea371f0e0",
--    "RejectionReason": 0
--}
	
	DECLARE	@IsSubmitted bit = JSON_VALUE(@json, '$.IsSubmitted')
	DECLARE	@SubmissionDate datetime = JSON_VALUE(@json, '$.SubmissionDate')
	DECLARE	@SubmittedBy nvarchar(50) = JSON_VALUE(@json, '$.SubmittedBy')
	DECLARE	@SendingStatus int = JSON_VALUE(@json, '$.SendingStatus')
	DECLARE	@SendDate datetime = JSON_VALUE(@json, '$.SendDate')
	DECLARE	@SendBy nvarchar(50) = JSON_VALUE(@json, '$.SendBy')
	DECLARE	@ApprovalStatus int = JSON_VALUE(@json, '$.ApprovalStatus')
	DECLARE	@ApprovalDate datetime = JSON_VALUE(@json, '$.ApprovalDate')
	DECLARE	@ApprovedBy nvarchar(50) = JSON_VALUE(@json, '$.ApprovedBy')
	DECLARE	@RejectionReason nvarchar(50) = JSON_VALUE(@json, '$.RejectionReason')
	DECLARE	@CorrectReason nvarchar(50) = JSON_VALUE(@json, '$.CorrectReason')
	DECLARE	@Comments nvarchar(200) = JSON_VALUE(@json, '$.Comments')
	DECLARE	@QuarterId uniqueidentifier = JSON_VALUE(@json, '$.QuarterId')

	INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[Log]
           ([Source]
           ,[Description])
     VALUES
           ('[BPG_FinOps_Invoice_Reimbursement].[sp_BulkCostUpdateJSON]'
           ,@json)

	update trans
	set 
	    IsSubmitted = case when @IsSubmitted IS NULL then IsSubmitted else @IsSubmitted end,
		SubmissionDate = case when @SubmissionDate IS NULL then SubmissionDate else @SubmissionDate end,
		SentForApproval = case when @SendingStatus IS NULL then SentForApproval else @SendingStatus end,
		SentForApprovalDate = case when @SendDate IS NULL then SentForApprovalDate else @SendDate end,
		ApprovalStatus = case when @ApprovalStatus IS NULL then ApprovalStatus else @ApprovalStatus end,
		DateApproved = case when @ApprovalDate IS NULL then DateApproved else @ApprovalDate end,
		CorrectReason = case when @CorrectReason IS NULL then CorrectReason else @CorrectReason end,
		RejectionReason = case when @RejectionReason IS NULL then RejectionReason else @RejectionReason end,
		Comments = case when @Comments IS NULL then Comments else @Comments end
	from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] trans
	where trans.Id in (
		select Id from OPENJSON(JSON_QUERY(@json, '$.Ids'))
		WITH (
		Id uniqueidentifier 'strict $.Id'
		)
	)

END
