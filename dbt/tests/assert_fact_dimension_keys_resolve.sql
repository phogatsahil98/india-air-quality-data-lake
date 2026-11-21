
SELECT

    f.fact_observation_key

FROM {{ ref('fct_station_pollutant_observation') }} f

LEFT JOIN {{ ref('dim_city') }} c

    ON f.city_key = c.city_key

LEFT JOIN {{ ref('dim_station') }} s

    ON f.station_key = s.station_key

LEFT JOIN {{ ref('dim_pollutant') }} p

    ON f.pollutant_id = p.pollutant_id

WHERE

    c.city_key IS NULL

    OR s.station_key IS NULL

    OR p.pollutant_id IS NULL