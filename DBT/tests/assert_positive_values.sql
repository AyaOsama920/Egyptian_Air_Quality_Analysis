select *
from {{ ref('fact_air_quality') }}
where value < 0
