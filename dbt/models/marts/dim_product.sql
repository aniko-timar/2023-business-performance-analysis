
with products as (
    select 
        product_name,
        category
    from {{ ref('int_sales_standardized') }}
    where product_name IS NOT NULL and category IS NOT NULL
    group by product_name, category
),

formatted as (
    select 
        lower(trim(product_name)) as product_name,
        lower(trim(category)) as category
    from products
)

select 
    md5(product_name) as product_id,
    product_name,
    category
from formatted