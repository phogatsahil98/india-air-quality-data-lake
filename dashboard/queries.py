"""SQL queries for the India Air Quality Intelligence dashboard.

All queries read dbt views in india_aqi_dbt_dev. Business logic remains in dbt.
"""
from __future__ import annotations


def sql_literal(value: str) -> str:
    """Safely quote a text literal for the small, controlled dashboard filters."""
    return "'" + str(value).replace("'", "''") + "'"


def where_filters(*, state: str | None = None, city: str | None = None, pollutant: str | None = None) -> str:
    clauses: list[str] = []
    if state and state != "All states":
        clauses.append(f"state = {sql_literal(state)}")
    if city and city != "All cities":
        clauses.append(f"city = {sql_literal(city)}")
    if pollutant and pollutant != "All pollutants":
        clauses.append(f"pollutant_id = {sql_literal(pollutant)}")
    return " AND ".join(clauses) if clauses else "1 = 1"


def overview_sql() -> str:
    return """
        SELECT
            extraction_id,
            ingestion_date,
            MAX(retrieved_at_utc) AS retrieved_at_utc,
            COUNT(*) AS total_observations,
            COUNT(DISTINCT city_key) AS city_count,
            COUNT(DISTINCT station_key) AS station_count,
            COUNT(DISTINCT pollutant_id) AS pollutant_count,
            COUNT_IF(is_analytically_usable) AS usable_observations,
            COUNT_IF(NOT is_analytically_usable) AS unusable_observations,
            COUNT_IF(measurement_status = 'UNAVAILABLE') AS unavailable_observations,
            ROUND(100.0 * COUNT_IF(is_analytically_usable) / NULLIF(COUNT(*), 0), 2)
                AS usable_measurement_pct
        FROM fct_station_pollutant_observation
        GROUP BY extraction_id, ingestion_date
        ORDER BY retrieved_at_utc DESC, extraction_id DESC
        LIMIT 1
    """


def city_pollutant_sql(*, state=None, city=None, pollutant=None) -> str:
    filters = where_filters(state=state, city=city, pollutant=pollutant)
    return f"""
        SELECT
            country, state, city, pollutant_id, pollutant_name, pollutant_category,
            ingestion_date, extraction_id, retrieved_at_utc,
            station_observation_count, usable_observation_count, unusable_observation_count,
            reporting_station_count, usable_station_count, measurement_availability_pct,
            mean_station_avg_value, highest_station_max_value, lowest_station_min_value
        FROM mart_city_pollutant_snapshot
        WHERE {filters}
        ORDER BY state, city, pollutant_id
        LIMIT 5000
    """


def city_completeness_sql(*, state=None, city=None) -> str:
    filters = where_filters(state=state, city=city)
    return f"""
        SELECT
            country, state, city, ingestion_date, extraction_id, retrieved_at_utc,
            station_pollutant_records, stations_reporting, pollutants_reported,
            available_measurements, unavailable_measurements, analytically_usable_measurements,
            measurement_availability_pct
        FROM mart_city_data_completeness
        WHERE {filters}
        ORDER BY measurement_availability_pct ASC, state, city
        LIMIT 2000
    """


def station_inventory_sql(*, state=None, city=None, pollutant=None) -> str:
    clauses: list[str] = []
    if state and state != "All states":
        clauses.append(f"s.state = {sql_literal(state)}")
    if city and city != "All cities":
        clauses.append(f"s.city = {sql_literal(city)}")
    if pollutant and pollutant != "All pollutants":
        clauses.append(f"f.pollutant_id = {sql_literal(pollutant)}")
    filters = " AND ".join(clauses) if clauses else "1 = 1"
    return f"""
        SELECT
            s.station_key, s.station_name, s.country, s.state, s.city,
            s.latitude, s.longitude,
            COUNT(*) AS pollutant_records,
            COUNT_IF(f.is_analytically_usable) AS usable_measurements
        FROM fct_station_pollutant_observation AS f
        INNER JOIN dim_station AS s ON f.station_key = s.station_key
        WHERE {filters}
          AND s.latitude IS NOT NULL
          AND s.longitude IS NOT NULL
        GROUP BY s.station_key, s.station_name, s.country, s.state, s.city,
                 s.latitude, s.longitude
        ORDER BY s.state, s.city, s.station_name
        LIMIT 2000
    """


def measurement_quality_sql() -> str:
    return """
        SELECT measurement_status, record_quality_reason, COUNT(*) AS record_count
        FROM fct_station_pollutant_observation
        GROUP BY measurement_status, record_quality_reason
        ORDER BY record_count DESC, measurement_status
    """


def snapshot_comparison_totals_sql() -> str:
    return """
        SELECT
            MAX(previous_extraction_id) AS previous_extraction_id,
            MAX(previous_retrieved_at_utc) AS previous_retrieved_at_utc,
            MAX(latest_extraction_id) AS latest_extraction_id,
            MAX(latest_retrieved_at_utc) AS latest_retrieved_at_utc,
            SUM(compared_entity_count) AS compared_entity_count,
            SUM(new_entity_count) AS new_entity_count,
            SUM(missing_entity_count) AS missing_entity_count,
            SUM(changed_entity_count) AS changed_entity_count,
            SUM(unchanged_entity_count) AS unchanged_entity_count,
            SUM(availability_changed_entity_count) AS availability_changed_entity_count,
            SUM(measurement_values_changed_entity_count) AS measurement_values_changed_entity_count
        FROM mart_cpcb_snapshot_change_summary
    """


def snapshot_comparison_by_pollutant_sql() -> str:
    return """
        SELECT
            pollutant_id,
            SUM(compared_entity_count) AS compared_entity_count,
            SUM(new_entity_count) AS new_entity_count,
            SUM(missing_entity_count) AS missing_entity_count,
            SUM(changed_entity_count) AS changed_entity_count,
            SUM(unchanged_entity_count) AS unchanged_entity_count,
            SUM(availability_changed_entity_count) AS availability_changed_entity_count
        FROM mart_cpcb_snapshot_change_summary
        GROUP BY pollutant_id
        ORDER BY pollutant_id
    """


def snapshot_change_detail_sql(*, state=None, city=None, pollutant=None, status=None) -> str:
    filters = where_filters(state=state, city=city, pollutant=pollutant)
    if status and status != "All statuses":
        filters += f" AND comparison_status = {sql_literal(status)}"
    return f"""
        SELECT
            country, state, city, station, pollutant_id, comparison_status,
            previous_measurement_status, latest_measurement_status,
            previous_avg_value, latest_avg_value, avg_value_delta, avg_value_pct_change,
            availability_changed, measurement_values_changed,
            previous_extraction_id, latest_extraction_id,
            previous_retrieved_at_utc, latest_retrieved_at_utc
        FROM mart_cpcb_snapshot_change_detail
        WHERE {filters}
        ORDER BY
            CASE comparison_status
                WHEN 'CHANGED' THEN 1
                WHEN 'NEW_IN_LATEST' THEN 2
                WHEN 'MISSING_FROM_LATEST' THEN 3
                ELSE 4
            END,
            ABS(COALESCE(avg_value_delta, 0)) DESC
        LIMIT 1000
    """
