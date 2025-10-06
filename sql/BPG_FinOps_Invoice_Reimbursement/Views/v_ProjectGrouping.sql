CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_ProjectGrouping]
as
with 
main as (
	select *,
		SUBSTRING(region_sector, 1, CHARINDEX('/', region_sector) - 1) AS region,
		SUBSTRING(region_sector, CHARINDEX('/', region_sector) + 1, LEN(region_sector)) AS sector
	from 
	BPG_FinOps_Invoice_Reimbursement.InvestmentContact
),
final as (
	select 
		a.project_code,
		b.fund_grouping as grouping
	from BPG_Fin_Ops.vw_Project_Listing_Invoice_Submission as a 

	left join BPG_FinOps_Invoice_Reimbursement.FundContact as b on 
		a.fund = b.fund

	where status in ('Fund', 'Sold', 'Dead','Ongoing','TBD')

	union all

	select
		a.project_code,
		case 
			when b.investment_grouping like 'Refer to Fund Contact' then c.fund_grouping 
		end as grouping
	from BPG_Fin_Ops.vw_Project_Listing_Invoice_Submission as a 

	left join main as b on 
		a.fund = b.fund
		and a.Sector_Region = b.sector
	left join BPG_FinOps_Invoice_Reimbursement.FundContact as c on 
		a.fund = c.fund

	where region_sector is not null
		and status in ('Closed')
		and b.investment_grouping like 'Refer to Fund Contact'

	union all 

	select
		a.project_code,
		b.investment_grouping as grouping
	from BPG_Fin_Ops.vw_Project_Listing_Invoice_Submission as a 
	left join main as b on 
		a.fund = b.fund
		and a.Sector_Region = b.sector
	left join BPG_FinOps_Invoice_Reimbursement.FundContact as c on 
		a.fund = c.fund
	where region_sector is not null
		and status in ('Closed')
		and b.investment_grouping not like '%Refer%'

	union all

	select 
		a.project_code,
		'' as grouping
	from BPG_Fin_Ops.vw_Project_Listing_Invoice_Submission as a 
	left join BPG_Fin_Ops.Fund as b on 
		a.fund = b.fund
	where status not in ('Fund', 'Sold', 'Dead','Closed','Ongoing','TBD')
) 

select distinct * from final