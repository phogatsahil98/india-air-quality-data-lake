
SELECT

    city_key,

    observation_date,

    extraction_id,

    COUNT(*) AS record_count

FROM {{ ref('mart_city_data_completeness') }}

GROUP BY

    city_key,

    observation_date,

    extraction_id

HAVING COUNT(*) > 1