{{ config(materialized='view') }}


WITH city_records AS (

    SELECT *

    FROM {{ ref('int_cpcb_observation_quality') }}

),

normalized_cities AS (

    SELECT

        LOWER(TRIM(country)) AS country_norm,

        LOWER(TRIM(state)) AS state_norm,

        city_normalized,

        MIN(country) AS country,

        MIN(state) AS state,

        MIN(city) AS city

    FROM city_records

    WHERE

        country IS NOT NULL

        AND state IS NOT NULL

        AND city_normalized IS NOT NULL

    GROUP BY

        LOWER(TRIM(country)),

        LOWER(TRIM(state)),

        city_normalized

)

SELECT

    CONCAT(
        country_norm,
        '|',
        state_norm,
        '|',
        city_normalized
    ) AS city_key,

    country,

    state,

    city,

    city_normalized

FROM normalized_cities