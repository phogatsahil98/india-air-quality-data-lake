select
    city_key,
    ingestion_date,
    extraction_id,
    count(*) as record_count
from {{ ref('mart_city_data_completeness') }}
group by city_key, ingestion_date, extraction_id
having count(*) > 1
