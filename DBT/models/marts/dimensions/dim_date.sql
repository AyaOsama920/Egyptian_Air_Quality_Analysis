{{ config(
    materialized='table',
    tags=['dimension']
) }}

with dates_from_facts as (
    select distinct date_utc 
    from {{ ref('stg_egypt_air_quality') }}
    
    union
    
    select distinct date_utc 
    from {{ ref('stg_nasa_air_quality') }}
),

enriched as (
    select
        -- Primary Key
        {{ dbt_utils.generate_surrogate_key(['date_utc']) }} as date_id,
        
        -- Date
        date_utc,
        
        -- Parts
        year(date_utc) as year,
        month(date_utc) as month,
        day(date_utc) as day,
        dayofweek(date_utc) as day_of_week_num,
        dayname(date_utc) as day_of_week,
        monthname(date_utc) as month_name,
        quarter(date_utc) as quarter,
        
        -- Flags
        case 
            when dayname(date_utc) in ('Saturday', 'Sunday') then true 
            else false 
        end as is_weekend,
        
        -- Season
        case 
            when month(date_utc) in (12, 1, 2) then 'Winter'
            when month(date_utc) in (3, 4, 5) then 'Spring'
            when month(date_utc) in (6, 7, 8) then 'Summer'
            when month(date_utc) in (9, 10, 11) then 'Fall'
        end as season,
        
        -- Relative dates
        datediff('day', date_utc, current_date()) as days_from_today
        
    from dates_from_facts
    where date_utc is not null
)

select * from enriched
order by date_utc