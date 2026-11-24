{{ config(materialized='view', tags=['cpcb', 'current_state', 'gold']) }}

with observations as (
    select *
    from {{ ref('int_cpcb_observation_quality') }}
),
enriched_observations as (
    select
        *,
        concat(
            lower(trim(country)), '|',
            lower(trim(state)), '|',
            city_normalized
        ) as derived_city_key
    from observations
)
select
    concat(o.extraction_id, '|', o.observation_id) as fact_observation_key,
    o.observation_id,
    c.city_key,
    s.station_key,
    p.pollutant_id,
    o.ingestion_date,
    o.retrieved_at_utc,
    o.source_last_update_raw,
    o.extraction_id,
    o.min_value,
    o.max_value,
    o.avg_value,
    o.measurement_status,
    o.record_quality_reason,
    o.is_analytically_usable,
    o.coordinate_status,
    o.range_consistency_status,
    o.source_occurrence_count,
    o.source_page_count
from enriched_observations as o
left join {{ ref('dim_city') }} as c
    on o.derived_city_key = c.city_key
left join {{ ref('dim_station') }} as s
    on o.station_key = s.station_key
left join {{ ref('dim_pollutant') }} as p
    on o.pollutant_id = p.pollutant_id
