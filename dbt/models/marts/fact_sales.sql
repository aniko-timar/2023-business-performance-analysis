
with sales_data as (
    select 
        transaction_id,
        transaction_date,
        lower(trim(product_name)) as product_name_clean,
        lower(trim(region)) as region_clean,
        lower(trim(customer_type)) as customer_type_clean,
        quantity,
        unit_price,
        revenue,
        cost
    from {{ ref('int_sales_standardized') }}
),

-- Pre-load dimension tables into CTEs for better performance
dim_date_cte as (
    select date_id, date_actual
    from {{ ref('dim_date') }}
),

dim_product_cte as (
    select product_id, product_name
    from {{ ref('dim_product') }}
),

dim_region_cte as (
    select region_id, region
    from {{ ref('dim_region') }}
),

dim_customer_type_cte as (
    select customer_type_id, customer_type
    from {{ ref('dim_customer_type') }}
),

-- Join everything in one pass
fact_sales as (
    select
        -- Transaction identifier
        s.transaction_id,
        
        -- Foreign keys to dimensions
        d.date_id,
        p.product_id,
        r.region_id,
        ct.customer_type_id,
        
        -- Measures
        s.quantity,
        s.unit_price,
        s.revenue,
        s.cost,
        
        -- Calculated measures
        s.revenue - s.cost as profit,
        case 
            when s.revenue > 0 
            then (s.revenue - s.cost) / s.revenue 
            else 0 
        end as profit_margin
        
    from sales_data s
    
    left join dim_date_cte d
        on s.transaction_date = d.date_actual
    
    left join dim_product_cte p
        on s.product_name_clean = p.product_name
    
    left join dim_region_cte r
        on s.region_clean = r.region
    
    left join dim_customer_type_cte ct
        on s.customer_type_clean = ct.customer_type
)

select * from fact_sales