# India Air Quality Intelligence Dashboard

A local Streamlit + Plotly dashboard that reads the existing dbt Gold views through Amazon Athena. It does not duplicate business logic in Python.

## Features

- National latest-snapshot KPIs.
- City/pollutant concentration and measurement-availability summaries.
- Monitoring station coordinates from `dim_station` joined to the current-state fact.
- Comparison of the latest two complete source extractions.
- Measurement-status and record-quality breakdowns.
- CSV exports for key views.

## Prerequisites

- Python 3.11+
- AWS CLI configured with IAM Identity Center profile `india-aqi-admin`.
- Athena workgroup `india-aqi-workgroup` in `ap-south-1`.
- dbt Gold views in database/schema `india_aqi_dbt_dev`.
- The configured Athena workgroup must have its results bucket/encryption configured.

## Install

From repository root:

```powershell
py -3.11 -m venv .venv-dashboard
.\.venv-dashboard\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r .\requirements-dashboard.txt
aws sso login --profile india-aqi-admin
$env:AWS_PROFILE = "india-aqi-admin"
$env:AWS_REGION = "ap-south-1"
$env:ATHENA_WORKGROUP = "india-aqi-workgroup"
$env:ATHENA_CATALOG = "AwsDataCatalog"
$env:ATHENA_DATABASE = "india_aqi_dbt_dev"
streamlit run .\dashboard\app.py
```

Open http://localhost:8501.

## Configuration

Environment variables are supported:

- `AWS_PROFILE` (default `india-aqi-admin`)
- `AWS_REGION` (default `ap-south-1`)
- `ATHENA_WORKGROUP` (default `india-aqi-workgroup`)
- `ATHENA_CATALOG` (default `AwsDataCatalog`)
- `ATHENA_DATABASE` (default `india_aqi_dbt_dev`)
- `ATHENA_QUERY_TIMEOUT_SECONDS` (default `240`)
- `ATHENA_POLL_INTERVAL_SECONDS` (default `2`)

The dashboard uses the Windows host's AWS profile and SSO token cache. Do not put access keys, secret keys, API keys or AWS credential files into this dashboard folder or commit them.

## Data semantics and limitations

- Current-state KPI, pollutant, station and quality views use the latest complete extraction via the canonical dbt models.
- Snapshot comparison uses the two latest complete extractions retained in the historical Silver dataset.
- `ingestion_date` is the UTC extraction/retrieval partition date. It is not interpreted as a CPCB observation date.
- City/station identity uses candidate keys derived from source labels; they are not official CPCB station identifiers.
- The dashboard presents source-reported pollutant concentration values. It does not calculate or certify official CPCB AQI.
- Snapshot-to-snapshot differences are not a continuous hourly time series.

## Cost and security

Each Athena query may incur scan costs. Queries select only needed columns, and Streamlit caches results for five minutes. Use the configured Athena workgroup and its scan controls. The dashboard is intended for local development/demo; do not expose it publicly without authentication, network controls and a secure deployment design.
