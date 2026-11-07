{{ config(
    materialized='table',
    tags=['dimension']
) }}

with locations_from_egypt as (
    select distinct
        city,
        region,
        latitude,
        longitude
    from {{ ref('stg_egypt_air_quality') }}
),

locations_from_nasa as (
    select distinct
        city,
        region,
        latitude,
        longitude
    from {{ ref('stg_nasa_air_quality') }}
),

combined as (
    select * from locations_from_egypt
    union
    select * from locations_from_nasa
),

enriched as (
    select
        -- Primary Key
        {{ dbt_utils.generate_surrogate_key(['city', 'latitude', 'longitude']) }} as location_id,
        
        -- Location details
        city,
        region,
        latitude,
        longitude,
        
        -- Calculated
        case 
            when city = 'Cairo' then 'Mega City'
            when city in ('Alexandria', 'Giza') then 'Major City'
            else 'Other City'
        end as city_size,
        
        -- Coordinates for mapping
        concat(latitude, ',', longitude) as coordinates
        
    from combined
    where city is not null
        and latitude is not null
        and longitude is not null
)

select * from enriched