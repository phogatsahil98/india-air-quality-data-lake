{{ config(materialized='view', tags=['cpcb', 'historical']) }}

with historical_snapshots as (
    select *
    from {{ ref('stg_cpcb_observation_snapshots') }}
),
latest_retrieval as (
    select max(retrieved_at_utc) as retrieved_at_utc
    from historical_snapshots
)

-- This selects the latest complete API extraction as a whole. It deliberately
-- does not select the latest row independently for each station-pollutant key:
-- doing that would keep entities that disappeared from the newest API response.
select snapshots.*
from historical_snapshots as snapshots
inner join latest_retrieval as latest
    on snapshots.retrieved_at_utc = latest.retrieved_at_utc
