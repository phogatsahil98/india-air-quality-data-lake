{{ config(materialized='view', tags=['cpcb', 'historical', 'gold']) }}

select
    candidate_entity_key,
    candidate_station_key,
    country,
    state,
    city,
    station,
    pollutant_id,
    previous_extraction_id,
    previous_retrieved_at_utc,
    latest_extraction_id,
    latest_retrieved_at_utc,
    comparison_status,
    previous_measurement_status,
    latest_measurement_status,
    measurement_values_changed,
    availability_changed,
    previous_min_value,
    latest_min_value,
    previous_max_value,
    latest_max_value,
    previous_avg_value,
    latest_avg_value,
    avg_value_delta,
    avg_value_pct_change
from {{ ref('int_cpcb_snapshot_entity_comparison') }}
