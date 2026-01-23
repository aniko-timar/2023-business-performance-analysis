with customer_types as (
    select customer_type
    from {{ ref('int_sales_standardized') }}
    where customer_type is not null
    group by customer_type
),

formatted as (
    select 
        lower(trim(customer_type)) as customer_type
    from customer_types
)

select 
    md5(customer_type) as customer_type_id,
    customer_type
from formatted