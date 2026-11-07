{{ config(
    materialized='table',
    tags=['dimension']
) }}

-- Static reference table for pollutants
select * from (
    values
        (1, 'CO', 'Carbon Monoxide', 'μg/m³', 10000, 'Reduces oxygen delivery to organs', 'High'),
        (2, 'NO', 'Nitric Oxide', 'μg/m³', null, 'Respiratory irritant', 'Medium'),
        (3, 'NO2', 'Nitrogen Dioxide', 'μg/m³', 40, 'Lung inflammation', 'High'),
        (4, 'O3', 'Ozone', 'μg/m³', 100, 'Respiratory issues', 'High'),
        (5, 'SO2', 'Sulfur Dioxide', 'μg/m³', 20, 'Breathing difficulties', 'High'),
        (6, 'PM2.5', 'Fine Particulate Matter', 'μg/m³', 15, 'Heart and lung diseases', 'Very High'),
        (7, 'PM10', 'Coarse Particulate Matter', 'μg/m³', 45, 'Respiratory problems', 'High'),
        (8, 'NH3', 'Ammonia', 'μg/m³', null, 'Eye and throat irritation', 'Low'),
        (9, 'AOD', 'Aerosol Optical Depth', 'dimensionless', null, 'Air clarity indicator', 'Medium')
) as pollutants(
    pollutant_id,
    pollutant_code,
    pollutant_name,
    unit,
    who_24h_limit,
    health_impact,
    severity
)