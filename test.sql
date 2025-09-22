alter view d.test 
as
with dd as (select id, name from prod), final as (select * from dd)
select id from final
