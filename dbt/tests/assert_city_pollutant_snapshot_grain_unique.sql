
SELECT

    city_key,

    pollutant_id,

    observation_date,

    extraction_id,

    COUNT(*) AS record_count

FROM {{ ref('mart_city_pollutant_snapshot') }}

GROUP BY

    city_key,

    pollutant_id,

    observation_date,

    extraction_id

HAVING COUNT(*) > 1