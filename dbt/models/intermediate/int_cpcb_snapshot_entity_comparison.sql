{{ config(materialized='view', tags=['cpcb', 'historical', 'snapshot_comparison']) }}

with extraction_runs as (
    select distinct
        extraction_id,
        retrieved_at_utc
    from {{ ref('stg_cpcb_observation_snapshots') }}
),
ranked_extractions as (
    select
        extraction_id,
        retrieved_at_utc,
        row_number() over (
            order by retrieved_at_utc desc, extraction_id desc
        ) as snapshot_rank
    from extraction_runs
),
latest_run as (
    select extraction_id, retrieved_at_utc
    from ranked_extractions
    where snapshot_rank = 1
),
previous_run as (
    select extraction_id, retrieved_at_utc
    from ranked_extractions
    where snapshot_rank = 2
),
latest_entities as (
    select s.*
    from {{ ref('stg_cpcb_observation_snapshots') }} as s
    inner join latest_run as r
        on s.extraction_id = r.extraction_id
),
previous_entities as (
    select s.*
    from {{ ref('stg_cpcb_observation_snapshots') }} as s
    inner join previous_run as r
        on s.extraction_id = r.extraction_id
),
joined as (
    select
        coalesce(l.candidate_entity_key, p.candidate_entity_key) as candidate_entity_key,
        coalesce(l.candidate_station_key, p.candidate_station_key) as candidate_station_key,
        coalesce(l.country, p.country) as country,
        coalesce(l.state, p.state) as state,
        coalesce(l.city, p.city) as city,
        coalesce(l.station, p.station) as station,
        coalesce(l.pollutant_id, p.pollutant_id) as pollutant_id,

        p.extraction_id as previous_extraction_id,
        p.retrieved_at_utc as previous_retrieved_at_utc,
        p.source_last_update_raw as previous_source_last_update_raw,
        p.measurement_status as previous_measurement_status,
        p.min_value as previous_min_value,
        p.max_value as previous_max_value,
        p.avg_value as previous_avg_value,

        l.extraction_id as latest_extraction_id,
        l.retrieved_at_utc as latest_retrieved_at_utc,
        l.source_last_update_raw as latest_source_last_update_raw,
        l.measurement_status as latest_measurement_status,
        l.min_value as latest_min_value,
        l.max_value as latest_max_value,
        l.avg_value as latest_avg_value,

        case
            when p.candidate_entity_key is null then 'NEW_IN_LATEST'
            when l.candidate_entity_key is null then 'MISSING_FROM_LATEST'
            when p.min_value is distinct from l.min_value
              or p.max_value is distinct from l.max_value
              or p.avg_value is distinct from l.avg_value
              or p.measurement_status is distinct from l.measurement_status
                then 'CHANGED'
            else 'UNCHANGED'
        end as comparison_status,

        case
            when p.candidate_entity_key is null or l.candidate_entity_key is null then null
            else (
                p.min_value is distinct from l.min_value
                or p.max_value is distinct from l.max_value
                or p.avg_value is distinct from l.avg_value
            )
        end as measurement_values_changed,

        case
            when p.candidate_entity_key is null or l.candidate_entity_key is null then null
            else p.measurement_status is distinct from l.measurement_status
        end as availability_changed,

        case
            when p.avg_value is not null and l.avg_value is not null
                then l.avg_value - p.avg_value
            else null
        end as avg_value_delta,

        case
            when p.avg_value is not null
             and l.avg_value is not null
             and p.avg_value <> 0
                then 100.0 * (l.avg_value - p.avg_value) / p.avg_value
            else null
        end as avg_value_pct_change

    from latest_entities as l
    full outer join previous_entities as p
        on l.candidate_entity_key = p.candidate_entity_key
)
select *
from joined
