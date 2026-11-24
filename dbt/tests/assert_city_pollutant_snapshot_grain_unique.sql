select
    city_key,
    pollutant_id,
    ingestion_date,
    extraction_id,
    count(*) as record_count
from {{ ref('mart_city_pollutant_snapshot') }}
group by city_key, pollutant_id, ingestion_date, extraction_id
having count(*) > 1
