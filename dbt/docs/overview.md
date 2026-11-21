
{% docs __overview__ %}

# India Air Quality Analytics

## Project objective

Build an AWS-based data lakehouse using government
air-quality observations published through CPCB and
the Government of India's Open Government Data portal.

## Architecture

CPCB API → Python ingestion → Amazon S3 Bronze
→ PySpark → Amazon S3 Silver
→ dbt → Gold analytical models
→ Amazon Athena → Analytics dashboard

## Data layers

### Bronze

Original CPCB API responses stored as JSON in Amazon S3.

### Silver

PySpark-processed observations stored as Parquet.

Includes:
- Numeric type conversion
- Missing-value standardization
- Exact duplicate removal
- Timestamp validation
- Coordinate validation
- Source provenance

### Gold

Analytics-ready dimensions, fact tables and aggregate marts.

Includes:
- City dimension
- Monitoring station dimension
- Pollutant dimension
- Station-pollutant observation fact
- City-pollutant snapshot mart
- City data-completeness mart

## Important analytical distinctions

Pollutant concentration and Air Quality Index (AQI)
are different measurements.

The current source snapshot contains one observation
timestamp per station-pollutant record. It does not
provide sufficient historical data for seasonal
trend analysis or official hourly AQI reconstruction.

AQI calculations will be implemented only after
validating pollutant units, CPCB breakpoints and
required temporal averaging windows.

## Technology stack

- Python
- Apache Spark / PySpark
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- dbt Core
- dbt-athena
- Apache Airflow

{% enddocs %}
