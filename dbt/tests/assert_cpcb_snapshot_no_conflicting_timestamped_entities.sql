-- Detect two different source records for the same candidate entity and
-- source-reported timestamp inside one extraction. Separate extractions remain
-- separate and are not considered conflicts.
select
    extraction_id,
    candidate_entity_key,
    source_last_update_raw,
    count(distinct source_record_hash) as distinct_source_records
from {{ ref('stg_cpcb_observation_snapshots') }}
group by extraction_id, candidate_entity_key, source_last_update_raw
having count(distinct source_record_hash) > 1
