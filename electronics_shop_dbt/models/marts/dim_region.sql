
with regions as (
    select region
    from {{ ref('int_sales_standardized') }}
    where region is not null
    group by region
),

formatted as (
    select 
        lower(trim(region)) as region
    from regions
)

select 
    md5(region) as region_id,
    region
from formatted