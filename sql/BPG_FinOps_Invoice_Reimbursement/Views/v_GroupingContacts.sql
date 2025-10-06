CREATE view [BPG_FinOps_Invoice_Reimbursement].[v_GroupingContacts]
as
with main as (
select *,
    SUBSTRING(region_sector, 1, CHARINDEX('/', region_sector) - 1) AS region,
    SUBSTRING(region_sector, CHARINDEX('/', region_sector) + 1, LEN(region_sector)) AS sector
from 
BPG_FinOps_Invoice_Reimbursement.InvestmentContact
)
,core as (
select
case when b.investment_grouping like 'Refer to Fund Contact' then c.fund_grouping end as grouping,
c.main_contact as sector_region_finance_lead, 
case when b.investment_grouping like 'Refer to Fund Contact' and c.other_contacts is null then c.escalation_contact_fund_controller_or_equivalent end as main_contact,
case when b.investment_grouping like 'Refer to Fund Contact'  and c.column9 is null then c.column8 end  as other_contacts,
'' as column7,
'' as column8, 
case when b.investment_grouping like 'Refer to Fund Contact'  and c.column10 is null then  c.column9 end  as column9,
case when b.investment_grouping like 'Refer to Fund Contact'  and c.column11 is null then c.column10 end  as column10,
case when b.investment_grouping like 'Refer to Fund Contact'  and c.column12 is null then c.column11 end  as column11,
case when b.investment_grouping like 'Refer to Fund Contact'  and c.column13 is null then  c.column12 end  as column12,
'' as column13,
'' as column14,
'' as column15,
'' as column16,
'' as column17,
'' as column18
 from BPG_Fin_Ops.vw_Project_Listing_Invoice_Submission as a 
left join main as b on a.fund = b.fund
and a.Sector_Region = b.sector
left join BPG_FinOps_Invoice_Reimbursement.FundContact as c on a.fund = c.fund
where region_sector is not null
and b.investment_grouping like 'Refer to Fund Contact'
union all 
select
b.investment_grouping as grouping,
b.sector_region_finance_lead, 
b.main_contact,
b.other_contacts,
b.column7,
b.column8, 
b.column9,
b.column10,
b.column11,
b.column12,
b.column13,
b.column14,
b.column15,
b.column16,
b.column17,
b.column18
 from BPG_Fin_Ops.vw_Project_Listing_Invoice_Submission as a 
left join main as b on a.fund = b.fund
and a.Sector_Region = b.sector
left join BPG_FinOps_Invoice_Reimbursement.FundContact as c on a.fund = c.fund
where region_sector is not null
and b.investment_grouping not like '%Refer%'
)
,investment as (
SELECT grouping,  
	main_contact as contact
     FROM core
	 
union all
SELECT grouping,  
	sector_region_finance_lead as contact
     FROM core
	union all
SELECT grouping,  
	other_contacts as contact
     FROM core
	 union all
SELECT grouping,  
	column7 as contact
     FROM core
	 union all
SELECT grouping,  
	column8 as contact
     FROM core
union all
SELECT grouping,  
	column9 as contact
     FROM core
	 union all
SELECT grouping,  
	column10 as contact
     FROM core
	union all
SELECT grouping,  
	column10 as contact
     FROM core
	union all
SELECT grouping,  
	column11 as contact
     FROM core
union all
SELECT grouping,  
	column12 as contact
     FROM core
	union all
SELECT grouping,  
	column13 as contact
     FROM core
	union all
SELECT grouping,  
	column14 as contact
     FROM core
union all
SELECT grouping,  
	column15 as contact
     FROM core
union all
SELECT grouping,  
	column16 as contact
     FROM core
union all
SELECT grouping,  
	column17 as contact
     FROM core
union all
SELECT grouping,  
	column18 as contact
     FROM core
)
, funds as (
select 
b.fund_grouping as grouping,
b.main_contact ,
b.escalation_contact_fund_controller_or_equivalent ,
b.other_contacts,
b.column8,
b.column9,
b.column10,
b.column11,
b.column12,
b.column13
from BPG_Fin_Ops.vw_Project_Listing_Invoice_Submission as a 
left join BPG_FinOps_Invoice_Reimbursement.FundContact as b on a.fund = b.fund
where status in ('Fund', 'Sold', 'Dead','Ongoing','TBD'))
, final as (
select 
grouping,
main_contact as contact from funds
union all 
select 
grouping,
escalation_contact_fund_controller_or_equivalent as contact from funds
union all 
select 
grouping,
other_contacts as contact from funds
union all 
select 
grouping,
column8 as contact from funds
union all 
select 
grouping,
column9 as contact from funds
union all 
select 
grouping,
column10 as contact from funds
union all 
select 
grouping,
column11 as contact from funds
union all 
select 
grouping,
column12 as contact from funds
union all 
select 
grouping,
column13 as contact from funds
union all
select * from investment) 

select distinct grouping, 
case when contact like '%<%' then substring(contact, CHARINDEX('<', contact)+1, CHARINDEX('>', contact)-CHARINDEX('<', contact)-1)
else contact
end as contact
from final where grouping is not null