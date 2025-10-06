
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_UpdateCost]
(
    @Id uniqueidentifier,
	@IsSubmitted bit = NULL,
	@SubmissionDate datetime = NULL,
	@SubmittedBy nvarchar(50) = NULL,
	@SendingStatus int = NULL,
	@SendDate datetime = NULL,
	@SendBy nvarchar(50) = NULL,
	@ApprovalStatus int = NULL,
	@ApprovalDate datetime = NULL,
	@ApprovedBy nvarchar(50) = NULL,
	@RejectionReason nvarchar(50) = NULL,
	@Comments nvarchar(200) = NULL
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

	INSERT INTO [BPG_FinOps_Invoice_Reimbursement].[Log]
           ([Source]
           ,[Description])
     VALUES
           ('[BPG_FinOps_Invoice_Reimbursement].[sp_UpdateCost]'
           ,'Begin args: @Id = ''' 
		    + CONVERT(NVARCHAR(50), @Id) + ''' @IsSubmitted = '''
		    + isnull(CONVERT(NVARCHAR(50), @IsSubmitted), 'NULL') + ''' @SubmissionDate = '''
			+ isnull(CONVERT(NVARCHAR(50), @SubmissionDate), 'NULL') + ''' @SubmittedBy = '''
			+ isnull(@SubmittedBy, 'NULL') + ''' @SendingStatus = '''
			+ isnull(CONVERT(NVARCHAR(50), @SendingStatus), 'NULL') + ''' @SendDate = '''
			+ isnull(CONVERT(NVARCHAR(50), @SendDate), 'NULL') + ''' @SendBy = '''
			+ isnull(@SendBy, 'NULL') + ''' @ApprovalStatus = '''
			+ isnull(CONVERT(NVARCHAR(50), @ApprovalStatus), 'NULL') + ''' @ApprovalDate = '''
			+ isnull(CONVERT(NVARCHAR(50), @ApprovalDate), 'NULL') + ''' @ApprovedBy = '''
			+ isnull(@ApprovedBy, 'NULL') + ''' @RejectionReason = '''
			+ isnull(@RejectionReason, 'NULL') + ''' @Comments = '''
			+ isnull(@Comments, 'NULL') + '''')

	update trans
	set 
	    IsSubmitted = case when @IsSubmitted IS NULL then IsSubmitted else @IsSubmitted end,
		SubmissionDate = case when @SubmissionDate IS NULL then SubmissionDate else @SubmissionDate end,
		SentForApproval = case when @SendingStatus IS NULL then SentForApproval else @SendingStatus end,
		SentForApprovalDate = case when @SendDate IS NULL then SentForApprovalDate else @SendDate end,
		ApprovalStatus = case when @ApprovalStatus IS NULL then ApprovalStatus else @ApprovalStatus end,
		DateApproved = case when @ApprovalDate IS NULL then DateApproved else @ApprovalDate end,
		RejectionReason = case when @RejectionReason IS NULL then RejectionReason else @RejectionReason end,
		Comments = case when @Comments IS NULL then Comments else @Comments end
	from [BPG_FinOps_Invoice_Reimbursement].[TransactionCost] trans
	where trans.Id = @Id
END
