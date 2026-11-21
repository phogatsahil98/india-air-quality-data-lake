{{ config(materialized='view', tags=['cpcb', 'historical', 'gold']) }}

select
    country,
    state,
    city,
    pollutant_id,
    max(previous_extraction_id) as previous_extraction_id,
    max(previous_retrieved_at_utc) as previous_retrieved_at_utc,
    max(latest_extraction_id) as latest_extraction_id,
    max(latest_retrieved_at_utc) as latest_retrieved_at_utc,

    count(*) as compared_entity_count,
    sum(case when comparison_status = 'NEW_IN_LATEST' then 1 else 0 end) as new_entity_count,
    sum(case when comparison_status = 'MISSING_FROM_LATEST' then 1 else 0 end) as missing_entity_count,
    sum(case when comparison_status = 'CHANGED' then 1 else 0 end) as changed_entity_count,
    sum(case when comparison_status = 'UNCHANGED' then 1 else 0 end) as unchanged_entity_count,
    sum(case when availability_changed then 1 else 0 end) as availability_changed_entity_count,
    sum(case when measurement_values_changed then 1 else 0 end) as measurement_values_changed_entity_count,
    avg(avg_value_delta) as mean_avg_value_delta,
    avg(abs(avg_value_delta)) as mean_absolute_avg_value_delta

from {{ ref('int_cpcb_snapshot_entity_comparison') }}
group by country, state, city, pollutant_id
