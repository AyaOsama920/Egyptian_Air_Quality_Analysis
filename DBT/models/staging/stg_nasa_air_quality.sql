with raw as (
    select *
    from {{ source('air_quality_source', 'AIR_QUALITY_NASA') }}
),

cleaned as (
    select
        -- Location (match with Egypt data)
        city,
        region,
        latitude,
        longitude,
        
        -- Temporal
        to_timestamp(date) as timestamp,
        date(date) as date_utc,
        year(date) as year,
        month(date) as month,
        day(date) as day,
        dayname(date) as day_of_week,
        
        -- Weather Metrics
        aod_55,
        temperature_c,
        precipitation_mm,
        humidity_percent,
        wind_speed_ms,
        
        -- Air Quality
        air_quality_category,
        pollution_score,
        
        -- Metadata
        data_source,
        data_type
        
    from raw
    where date is not null
        and city is not null  -- Important: no nulls!
)

select * from cleaned