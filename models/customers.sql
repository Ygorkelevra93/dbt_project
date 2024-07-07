-- INICIO DO WITH
with markup as 
-- aqui faz a regra de verificação de duplicatas , e resultado faz a ultima coluna result , ficar igual
(
select *,
first_value(customer_id)
over(partition by company_name, contact_name 
order by company_name
rows between unbounded preceding and unbounded following) as result
from {{ source("sources", "customers") }}
), 
-- aqui chama essa ultima coluna da tabela de cima
removed as 
(
    select distinct result from markup
),
final as (
    select * from {{ source("sources", "customers") }} where customer_id in (select result from removed)
)
-- fim do WITH

select * from final
