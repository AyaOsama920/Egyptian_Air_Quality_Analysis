{{ config(
    materialized='table',
    tags=['fact']
) }}

with base as (
    select * from {{ ref('stg_nasa_air_quality') }}
),

with_keys as (
    select
        -- Surrogate Keys
        {{ dbt_utils.generate_surrogate_key(['date_utc']) }} as date_id,
        {{ dbt_utils.generate_surrogate_key(['city', 'latitude', 'longitude']) }} as location_id,
        
        -- Temporal
        date_utc,
        year,
        month,
        day,
        day_of_week,
        
        -- Weather Metrics
        aod_55 as aerosol_optical_depth,
        temperature_c,
        precipitation_mm,
        humidity_percent,
        wind_speed_ms,
        
        -- Air Quality from Satellite
        air_quality_category,
        pollution_score,
        
        -- Calculated fields
        case 
            when temperature_c > 30 then 'Hot'
            when temperature_c > 20 then 'Warm'
            when temperature_c > 10 then 'Mild'
            else 'Cold'
        end as temp_category,
        
        case 
            when humidity_percent > 70 then 'High'
            when humidity_percent > 40 then 'Moderate'
            else 'Low'
        end as humidity_level,
        
        -- Metadata
        data_source,
        data_type
        
    from base
)

select * from with_keys