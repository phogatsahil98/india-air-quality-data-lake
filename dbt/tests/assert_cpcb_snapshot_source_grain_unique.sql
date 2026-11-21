-- A source record hash can recur in a later extraction, so uniqueness is scoped
-- to extraction_id + source_record_hash, never to source_record_hash alone.
select
    extraction_id,
    source_record_hash,
    count(*) as row_count
from {{ ref('stg_cpcb_observation_snapshots') }}
group by extraction_id, source_record_hash
having count(*) > 1
