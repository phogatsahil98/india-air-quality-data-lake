
SELECT

    observation_id

FROM {{ ref('int_cpcb_usable_measurements') }}

WHERE

    min_value IS NULL

    OR max_value IS NULL

    OR avg_value IS NULL