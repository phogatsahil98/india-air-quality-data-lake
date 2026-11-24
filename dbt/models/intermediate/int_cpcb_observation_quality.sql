{{ config(materialized='view', tags=['cpcb', 'current_state', 'quality']) }}

with latest_observations as (
    select *
    from {{ ref('int_cpcb_latest_snapshot') }}
),
range_assessment as (
    select
        *,
        case
            when measurement_status <> 'AVAILABLE'
              or min_value is null or avg_value is null or max_value is null then 'NOT_EVALUATED'
            when min_value <= avg_value and avg_value <= max_value then 'CONSISTENT'
            else 'INCONSISTENT'
        end as range_consistency_status
    from latest_observations
),
quality_assessment as (
    select
        *,
        case
            when measurement_status = 'UNAVAILABLE' then 'MEASUREMENT_UNAVAILABLE'
            when measurement_status = 'PARTIAL' then 'PARTIAL_MEASUREMENT'
            when measurement_status = 'INVALID_NUMERIC' then 'INVALID_MEASUREMENT'
            when coordinate_status <> 'VALID' then 'INVALID_COORDINATES'
            when min_value is null or max_value is null or avg_value is null then 'MISSING_NUMERIC_VALUES'
            when range_consistency_status = 'INCONSISTENT' then 'INCONSISTENT_MEASUREMENTS'
            else 'VALID'
        end as record_quality_reason
    from range_assessment
)
select
    *,
    case when record_quality_reason = 'VALID' then true else false end as is_analytically_usable,
    case when measurement_status = 'AVAILABLE' then true else false end as is_measurement_available,
    case when coordinate_status = 'VALID' then true else false end as is_coordinate_valid
from quality_assessment
