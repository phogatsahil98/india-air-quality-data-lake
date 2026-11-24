{{ config(materialized='view', tags=['cpcb', 'historical', 'current_state']) }}

with extraction_runs as (
    select
        extraction_id,
        max(retrieved_at_utc) as retrieved_at_utc
    from {{ ref('stg_cpcb_observation_snapshots') }}
    group by extraction_id
),
ranked_extractions as (
    select
        extraction_id,
        retrieved_at_utc,
        row_number() over (
            order by retrieved_at_utc desc, extraction_id desc
        ) as extraction_rank
    from extraction_runs
),
latest_extraction as (
    select extraction_id, retrieved_at_utc
    from ranked_extractions
    where extraction_rank = 1
)

-- Select the latest complete extraction as one unit. Do not choose a latest
-- row independently per entity, because that would carry forward entities
-- that are absent from the most recent API response.
select snapshots.*
from {{ ref('stg_cpcb_observation_snapshots') }} as snapshots
inner join latest_extraction as latest
    on snapshots.extraction_id = latest.extraction_id
   and snapshots.retrieved_at_utc = latest.retrieved_at_utc
