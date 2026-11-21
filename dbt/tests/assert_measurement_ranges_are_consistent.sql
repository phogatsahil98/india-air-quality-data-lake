
SELECT

    observation_id,

    min_value,
    avg_value,
    max_value

FROM {{ ref('int_cpcb_usable_measurements') }}

WHERE

    NOT (
        min_value <= avg_value
        AND avg_value <= max_value
    )