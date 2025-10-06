-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [BPG_FinOps_Invoice_Reimbursement].[sp_GenerateQuarters]
AS
BEGIN
    SET NOCOUNT ON

    declare @year int
	set @year = isnull((select year(max(EndDate)) from [BPG_FinOps_Invoice_Reimbursement].[Quarter]), 2024)

	declare @endYear int
	set @endYear = year(getdate()) + 10

	while @year <= @endYear 
	begin

		declare @quarterNum int
		set @quarterNum = 1

		while @quarterNum <= 4 
		begin

		     declare @quarter nvarchar(50)
			 set @quarter = cast(@year as nvarchar(4)) + ', Q' + cast(@quarterNum as nvarchar(1))

			-- print @quarter

			 if (select top 1 q.[Name] from [BPG_FinOps_Invoice_Reimbursement].[Quarter] q where q.Name = @quarter) is null
			 begin
			     declare @startMonth int
				 set @startMonth = 1 + (@quarterNum - 1) * 3
				 declare @endMonth int
				 set @endMonth = @quarterNum * 3

				 declare @startDay int
				 set @startDay = 1
				 declare @endDay int
				 set @endDay = 30

				 if @quarterNum <> 2 and @quarterNum <> 3
				 begin
				     set @endDay = @endDay + 1
				 end

				-- print cast(@year as nvarchar(4)) + '-' + cast(@startMonth as nvarchar(2)) + '-' + cast(@startDay as nvarchar(2))
				-- print cast(@year as nvarchar(4)) + '-' + cast(@endMonth as nvarchar(2)) + '-' + cast(@endDay as nvarchar(2))

				 declare @startDate date
				 set @startDate = DATEFROMPARTS(@year, @startMonth, @startDay)
				 declare @endDate date
				 set @endDate = DATEFROMPARTS(@year, @endMonth, @endDay)

			     insert into [BPG_FinOps_Invoice_Reimbursement].[Quarter] ([Name], [StartDate]  ,[EndDate])
				 values (@quarter, @startDate, @endDate)

				-- print @quarter + cast(@startDate as nvarchar(10)) + cast(@endDate as nvarchar(10))

			 end

			 set @quarterNum = @quarterNum + 1
		end

		set @year = @year + 1
	end
END
