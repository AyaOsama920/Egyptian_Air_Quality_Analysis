{{ config(
    materialized='table',
    tags=['fact']
) }}

with base as (
    select * from {{ ref('stg_egypt_air_quality') }}
),

-- Add surrogate keys for dimension relationships
with_keys as (
    select
        -- Surrogate Keys (FKs)
        {{ dbt_utils.generate_surrogate_key(['date_utc']) }} as date_id,
        {{ dbt_utils.generate_surrogate_key(['city', 'latitude', 'longitude']) }} as location_id,
        
        -- Time dimensions
        timestamp,
        date_utc,
        hour,
        case 
            when hour between 6 and 11 then 'Morning'
            when hour between 12 and 17 then 'Afternoon'
            when hour between 18 and 21 then 'Evening'
            else 'Night'
        end as time_of_day,
        day_of_week,
        
        -- Air Quality Index
        aqi,
        aqi_category,
        
        -- Pollutants (as columns - NOT unpivoted!)
        co,
        no,
        no2,
        o3,
        so2,
        pm2_5,
        pm10,
        nh3,
        
        -- Calculated measures
        case 
            when pm2_5 > 15 then true 
            else false 
        end as pm25_exceeds_who_limit,
        
        case 
            when pm10 > 45 then true 
            else false 
        end as pm10_exceeds_who_limit,
        
        -- Metadata
        data_source,
        data_type
        
    from base
)

select * from with_keys