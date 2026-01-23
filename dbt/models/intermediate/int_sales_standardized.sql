WITH staged AS (
    SELECT
        *
    FROM
        {{ ref('stg_sales') }}
),

standardized AS (

    SELECT
        UPPER(transaction_id) AS transaction_id,
        transaction_date,
        
        CASE 
            WHEN UPPER(customer_type) = 'NEW' THEN 'New'
            WHEN UPPER(customer_type) = 'RETURNING' THEN 'Returning'
            WHEN UPPER(customer_type) = 'VIP' THEN 'VIP'
            ELSE 'Other'
        END AS customer_type,
        
        CASE
            WHEN LOWER(product_name) LIKE '%laptop%' THEN 'Laptop'
            WHEN LOWER(product_name) LIKE '%mouse%' THEN 'Mouse'
            WHEN LOWER(product_name) LIKE '%keyboard%' THEN 'Keyboard'
            WHEN LOWER(product_name) LIKE '%monitor%' THEN 'Monitor'
            WHEN LOWER(product_name) LIKE '%headphone%' THEN 'Headphones'
            WHEN LOWER(product_name) LIKE '%webcam%' THEN 'Webcam'
            WHEN LOWER(product_name) LIKE '%usb%' THEN 'USB Cable'
            WHEN LOWER(product_name) LIKE '%charger%' AND LOWER(product_name) NOT LIKE '%laptop%' THEN 'Charger'
            ELSE 'Other'
        END AS product_name,

        CASE
            WHEN LOWER(product_name) LIKE '%laptop%' THEN 'Computers'
            WHEN LOWER(product_name) LIKE '%mouse%' OR LOWER(product_name) LIKE '%keyboard%' THEN 'Peripherals'
            WHEN LOWER(product_name) LIKE '%monitor%' THEN 'Display'
            WHEN LOWER(product_name) LIKE '%headphone%' OR LOWER(product_name) LIKE '%webcam%' THEN 'Audio and Video'
            WHEN LOWER(product_name) LIKE '%usb%' OR (LOWER(product_name) LIKE '%charger%' AND LOWER(product_name) NOT LIKE '%laptop%') THEN 'Cables'
            ELSE 'Other'
        END AS category,
        
        -- CASE
        --     WHEN UPPER(category) IN ('ELECTRONICS', 'COMPUTERS') THEN 'Electronics'
        --     WHEN UPPER(category) IN ('ACCESSORIES', 'PERIPHERALS') THEN 'Accessories'
        --     WHEN UPPER(category) IN ('DISPLAYS') THEN 'Displays'
        --     WHEN UPPER(category) IN ('AUDIO') THEN 'Audio'
        --     WHEN UPPER(category) IN ('VIDEO') THEN 'Video'
        --     WHEN UPPER(category) IN ('CABLES') THEN 'Cables'
        --     WHEN UPPER(category) IN ('POWER') THEN 'Power'
        --     ELSE 'Other'
        -- END AS category,
        
        CASE
            WHEN UPPER(region) IN ('NORTH', 'N') THEN 'North'
            WHEN UPPER(region) IN ('SOUTH', 'S') THEN 'South'
            WHEN UPPER(region) IN ('EAST', 'E') THEN 'East'
            WHEN UPPER(region) IN ('WEST', 'W') THEN 'West'
            WHEN UPPER(region) IN ('CENTRAL', 'C') THEN 'Central'
            ELSE NULL
        END AS region,
        
        quantity,
        unit_price,
        revenue,
        cost

    FROM staged

),

with_business_logic AS (

    SELECT
        *,
        
        -- Calculate profit
        revenue - cost AS profit,
        
        -- Calculate profit margin
        CASE 
            WHEN revenue > 0 THEN ROUND((revenue - cost) / revenue, 4)
            ELSE NULL 
        END AS profit_margin,
        
        -- Data quality flags
        CASE 
            WHEN transaction_date IS NULL THEN TRUE
            WHEN quantity IS NULL OR quantity <= 0 THEN TRUE
            WHEN revenue IS NULL OR revenue <= 0 THEN TRUE
            WHEN cost IS NULL THEN TRUE
            ELSE FALSE
        END AS is_invalid_record

    FROM standardized

),

deduplicated AS (

    SELECT *
    FROM with_business_logic
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY transaction_id 
        ORDER BY transaction_date, revenue DESC
    ) = 1

),

final AS (

    SELECT *
    FROM deduplicated
    WHERE is_invalid_record = FALSE

)

SELECT * FROM final