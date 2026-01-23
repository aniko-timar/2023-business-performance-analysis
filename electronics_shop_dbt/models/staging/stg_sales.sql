with

source as (

    select * from {{ source('electronics_db', 'RAW_SALES_TRANSACTIONS') }}

),

renamed AS (

    SELECT
        -- IDs
        "transaction_id" as transaction_id,
        TRY_TO_DATE("transaction_date") AS transaction_date,
        
        -- Attributes  
        "customer_type" as customer_type,
        "product_name" as product_name,
        "category" as category,
        "region" as region,
        
        -- Measures
        "quantity" as quantity,
        "unit_price" as unit_price,
        "revenue" as revenue,
        "cost" as cost

    FROM source

),

cleaned AS (

    SELECT
        TRIM(transaction_id) AS transaction_id,
        transaction_date,
        TRIM(customer_type) AS customer_type,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(region) AS region,
        
        quantity::INTEGER AS quantity,
        unit_price::FLOAT AS unit_price,
        revenue::FLOAT AS revenue,
        cost::FLOAT AS cost

    FROM renamed
    
    WHERE transaction_id IS NOT NULL

)

SELECT * FROM cleaned