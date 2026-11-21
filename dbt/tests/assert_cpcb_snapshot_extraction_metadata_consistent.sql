-- Each extraction ID should map to exactly one retrieval timestamp and date partition.
select
    extraction_id,
    count(distinct retrieved_at_utc) as retrieval_timestamp_count,
    count(distinct ingestion_date) as ingestion_date_count
from {{ ref('stg_cpcb_observation_snapshots') }}
group by extraction_id
having count(distinct retrieved_at_utc) <> 1
    or count(distinct ingestion_date) <> 1
