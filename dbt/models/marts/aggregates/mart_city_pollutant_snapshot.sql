{{ config(materialized='view', tags=['cpcb', 'current_state', 'gold']) }}

select
    f.city_key,
    c.country,
    c.state,
    c.city,
    f.pollutant_id,
    p.pollutant_name,
    p.pollutant_category,
    f.ingestion_date,
    f.extraction_id,
    max(f.retrieved_at_utc) as retrieved_at_utc,
    count(*) as station_observation_count,
    count_if(f.is_analytically_usable) as usable_observation_count,
    count_if(not f.is_analytically_usable) as unusable_observation_count,
    count(distinct f.station_key) as reporting_station_count,
    count(distinct case when f.is_analytically_usable then f.station_key end) as usable_station_count,
    round(
        100.0 * count_if(f.is_analytically_usable) / nullif(count(*), 0),
        2
    ) as measurement_availability_pct,
    round(avg(case when f.is_analytically_usable then f.avg_value end), 2) as mean_station_avg_value,
    max(case when f.is_analytically_usable then f.max_value end) as highest_station_max_value,
    min(case when f.is_analytically_usable then f.min_value end) as lowest_station_min_value
from {{ ref('fct_station_pollutant_observation') }} as f
inner join {{ ref('dim_city') }} as c
    on f.city_key = c.city_key
inner join {{ ref('dim_pollutant') }} as p
    on f.pollutant_id = p.pollutant_id
group by
    f.city_key,
    c.country,
    c.state,
    c.city,
    f.pollutant_id,
    p.pollutant_name,
    p.pollutant_category,
    f.ingestion_date,
    f.extraction_id
