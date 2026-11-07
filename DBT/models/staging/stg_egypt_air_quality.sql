with raw as (
    select *
    from {{ source('air_quality_source', 'AIR_QUALITY') }}
),

cleaned as (
    select
        -- Location
        city,
        region,
        latitude,
        longitude,
        
        -- Temporal
        timestamp,
        date(timestamp) as date_utc,
        hour(timestamp) as hour,
        dayname(timestamp) as day_of_week,
        
        -- Air Quality Index
        aqi,
        
        -- Pollutants (keep them as columns!)
        co,
        no,
        no2,
        o3,
        so2,
        pm2_5,
        pm10,
        nh3,
        
        -- Metadata
        data_source,
        data_type,
        aqi_category
        
    from raw
    where timestamp is not null
        and aqi is not null
)

select * from cleaned