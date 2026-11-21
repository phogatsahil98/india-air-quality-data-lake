-- The latest-snapshot model must represent one complete extraction, not a
-- per-entity union of records from different extraction runs.
select
    count(distinct extraction_id) as extraction_count
from {{ ref('int_cpcb_latest_snapshot') }}
having count(distinct extraction_id) <> 1
