-- Current-state fact must preserve every row in the latest complete snapshot.
-- The fact applies enrichment joins, but those joins are left joins and must
-- not add or remove source observations.
with latest_snapshot_count as (
    select count(*) as row_count
    from {{ ref('int_cpcb_latest_snapshot') }}
),
fact_count as (
    select count(*) as row_count
    from {{ ref('fct_station_pollutant_observation') }}
)
select
    latest_snapshot_count.row_count as expected_row_count,
    fact_count.row_count as actual_row_count
from latest_snapshot_count
cross join fact_count
where latest_snapshot_count.row_count <> fact_count.row_count
