
SELECT

    city_key,

    measurement_availability_pct

FROM {{ ref('mart_city_pollutant_snapshot') }}

WHERE

    measurement_availability_pct < 0

    OR measurement_availability_pct > 100