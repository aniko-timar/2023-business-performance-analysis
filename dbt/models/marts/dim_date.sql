
with date_range as (
    select
        '{{ var("date_dimension_start") }}'::date as start_date,
        '{{ var("date_dimension_end") }}'::date as end_date
),

date_spine as (
    select
        dateadd(
            day, 
            row_number() over (order by seq4()) - 1, 
            (select start_date from date_range)
        )::date as date_day
    from table(generator(rowcount => 10000))
    qualify date_day <= (select end_date from date_range)
),

date_dimension as (
    select
        -- Primary Key
        md5(date_day) as date_id,
        
        -- Actual Date
        date_day as date_actual,
        
        -- Year Attributes
        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        day(date_day) as day,
        
        -- Month Details
        decode(monthname(date_day),
            'Jan', 'January',
            'Feb', 'February',
            'Mar', 'March',
            'Apr', 'April',
            'May', 'May',
            'Jun', 'June',
            'Jul', 'July',
            'Aug', 'August',
            'Sep', 'September',
            'Oct', 'October',
            'Nov', 'November',
            'Dec', 'December'
        ) as month_name,
        monthname(date_day) as month_short_name,  -- Jan, Feb, Mar
        to_char(date_day, 'YYYY-MM') as year_month,
        
        -- Week Attributes
        weekofyear(date_day) as week_of_year,
        date_trunc('week', date_day)::date as week_start_date,
        
        -- Day Attributes
        dayofweek(date_day) as day_of_week,  -- 0=Sunday, 1=Monday, 6=Saturday
        decode(dayname(date_day),
            'Mon', 'Monday',
            'Tue', 'Tuesday', 
            'Wed', 'Wednesday',
            'Thu', 'Thursday',
            'Fri', 'Friday',
            'Sat', 'Saturday',
            'Sun', 'Sunday'
        ) as day_name,
        dayname(date_day) as day_short_name,  -- Mon, Tue, Wed
        dayofyear(date_day) as day_of_year,
        
        -- Boolean Flags (Snowflake default: 0=Sunday, 6=Saturday)
        case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend,  -- Sunday=0, Saturday=6
        case when dayofweek(date_day) between 1 and 5 then true else false end as is_weekday,  -- Monday=1 to Friday=5
        
        -- Formatted Strings
        to_char(date_day, 'YYYY-MM-DD') as date_string,
        to_number(to_char(date_day, 'YYYYMMDD')) as date_int,
        
        -- Quarter Display
        year(date_day) || '-Q' || quarter(date_day) as year_quarter,
        
        -- Fiscal Year (adjust based on your fiscal calendar)
        -- Example: Fiscal year starts in April
        case 
            when month(date_day) >= 4 
            then year(date_day)
            else year(date_day) - 1 
        end as fiscal_year,
        
        case 
            when month(date_day) in (4, 5, 6) then 1
            when month(date_day) in (7, 8, 9) then 2
            when month(date_day) in (10, 11, 12) then 3
            else 4
        end as fiscal_quarter,
        
        -- Relative Date Flags
        case when date_day = current_date() then true else false end as is_today,
        case when date_day = dateadd(day, -1, current_date()) then true else false end as is_yesterday,
        case when date_day between date_trunc('week', current_date()) and current_date() then true else false end as is_current_week,
        case when date_trunc('month', date_day) = date_trunc('month', current_date()) then true else false end as is_current_month,
        case when year(date_day) = year(current_date()) then true else false end as is_current_year

    from date_spine
)

select * from date_dimension