{{ config( materialized='view' ) }}


WITH observations AS (

    SELECT *

    FROM {{ ref('stg_cpcb_observations') }}

),

quality_assessment AS (

    SELECT

        *,

        CASE

            WHEN measurement_status = 'UNAVAILABLE'
                THEN 'MEASUREMENT_UNAVAILABLE'

            WHEN measurement_status = 'PARTIAL'
                THEN 'PARTIAL_MEASUREMENT'

            WHEN measurement_status = 'INVALID'
                THEN 'INVALID_MEASUREMENT'

            WHEN coordinate_status != 'VALID'
                THEN 'INVALID_COORDINATES'

            WHEN timestamp_parse_status != 'VALID'
                THEN 'INVALID_TIMESTAMP'

            WHEN range_consistency_status != 'CONSISTENT'
                THEN 'INCONSISTENT_MEASUREMENTS'

            WHEN min_value IS NULL
                OR max_value IS NULL
                OR avg_value IS NULL
                THEN 'MISSING_NUMERIC_VALUES'

            ELSE 'VALID'

        END AS record_quality_reason

    FROM observations

)

SELECT

    *,

    CASE

        WHEN record_quality_reason = 'VALID'
            THEN TRUE

        ELSE FALSE

    END AS is_analytically_usable

FROM quality_assessment