{{ config(materialized='view', tags=['cpcb', 'current_state', 'quality']) }}

select
    observation_id,
    source_record_hash,
    candidate_entity_key,
    station_key,
    candidate_station_key,
    country,
    state,
    city,
    city_normalized,
    station,
    source_last_update_raw,
    retrieved_at_utc,
    ingestion_date,
    extraction_id,
    latitude,
    longitude,
    coordinate_status,
    pollutant_id,
    min_value,
    max_value,
    avg_value,
    measurement_status,
    range_consistency_status,
    record_quality_reason,
    is_analytically_usable,
    source_page_count,
    source_occurrence_count,
    duplicate_excess_count
from {{ ref('int_cpcb_observation_quality') }}
where is_analytically_usable = true
