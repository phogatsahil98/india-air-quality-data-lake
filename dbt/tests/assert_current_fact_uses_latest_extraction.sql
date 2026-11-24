-- The current-state star schema must contain records from the latest complete
-- extraction only. Older extractions are used by snapshot comparison models.
with latest_extractions as (
    select distinct extraction_id
    from {{ ref('int_cpcb_latest_snapshot') }}
),
violations as (
    select distinct f.extraction_id
    from {{ ref('fct_station_pollutant_observation') }} as f
    left join latest_extractions as l
        on f.extraction_id = l.extraction_id
    where l.extraction_id is null
)
select *
from violations
