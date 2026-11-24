from __future__ import annotations

import os
import pandas as pd
import plotly.express as px
import streamlit as st

from athena_client import AthenaQueryError, run_athena_query
from queries import (
    city_completeness_sql,
    city_pollutant_sql,
    measurement_quality_sql,
    overview_sql,
    snapshot_change_detail_sql,
    snapshot_comparison_by_pollutant_sql,
    snapshot_comparison_totals_sql,
    station_inventory_sql,
)


st.set_page_config(
    page_title="India Air Quality Intelligence",
    page_icon="🌫️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.6rem; padding-bottom: 2.4rem; }
      [data-testid="stMetric"] { background: rgba(128,128,128,.08); padding: 14px 16px; border-radius: 12px; }
      .dashboard-subtitle { color: #9aa4b2; font-size: .94rem; margin-top: -.45rem; }
      .section-note { color: #9aa4b2; font-size: .82rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300, show_spinner="Querying Amazon Athena…")
def query_df(sql: str) -> pd.DataFrame:
    df, _query_id = run_athena_query(sql)
    return df


def to_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def first_row(df: pd.DataFrame) -> dict:
    return {} if df.empty else df.iloc[0].to_dict()


def fmt_int(value) -> str:
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return "—"


def fmt_pct(value) -> str:
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "—"


def display_query_error(exc: Exception) -> None:
    st.error(str(exc))
    st.info(
        "This error was returned by Athena. First inspect the SQL error and query ID above: "
        "COLUMN_NOT_FOUND, invalid ORDER BY references, and missing table/column errors are "
        "SQL or schema issues, not SSO failures. Refresh the IAM Identity Center session only "
        "when the error indicates expired credentials or access denied, using "
        "`aws sso login --profile india-aqi-admin` in PowerShell."
    )


st.title("🌫️ India Air Quality Intelligence")
st.markdown(
    '<p class="dashboard-subtitle">CPCB monitoring coverage, reported pollutant measurements, '
    'snapshot changes and data quality — powered by AWS Athena and dbt Gold.</p>',
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Dashboard controls")
    st.caption("Source: Amazon Athena · dbt Gold views")
    st.caption(f"AWS profile: `{os.getenv('AWS_PROFILE', 'india-aqi-admin')}`")
    st.caption(f"Region: `{os.getenv('AWS_REGION', 'ap-south-1')}`")
    if st.button("Refresh dashboard data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.caption("Filters apply to city, pollutant and station-level views. National KPIs remain unfiltered.")

try:
    overview_df = query_df(overview_sql())
    if overview_df.empty:
        st.warning("No current-state fact records were returned. Run the Airflow pipeline and dbt build first.")
        st.stop()
    overview = first_row(overview_df)

    cities_for_filter = query_df(
        "SELECT DISTINCT state, city FROM mart_city_data_completeness ORDER BY state, city LIMIT 3000"
    )
    pollutant_filter_df = query_df(
        "SELECT DISTINCT pollutant_id FROM mart_city_pollutant_snapshot ORDER BY pollutant_id"
    )

    states = ["All states"] + sorted(cities_for_filter["state"].dropna().astype(str).unique().tolist())
    with st.sidebar:
        selected_state = st.selectbox("State / Union Territory", states)
        if selected_state == "All states":
            city_options = ["All cities"] + sorted(cities_for_filter["city"].dropna().astype(str).unique().tolist())
        else:
            city_options = ["All cities"] + sorted(
                cities_for_filter.loc[cities_for_filter["state"] == selected_state, "city"]
                .dropna().astype(str).unique().tolist()
            )
        selected_city = st.selectbox("City", city_options)
        pollutants = ["All pollutants"] + pollutant_filter_df["pollutant_id"].dropna().astype(str).tolist()
        selected_pollutant = st.selectbox("Pollutant", pollutants)

    ingestion_date = str(overview.get("ingestion_date", "—"))
    extraction_id = str(overview.get("extraction_id", "—"))
    retrieved_at = str(overview.get("retrieved_at_utc", "—"))
    st.caption(f"Latest complete extraction: **{extraction_id}** · Ingestion date: **{ingestion_date}** · Retrieved (UTC): **{retrieved_at}**")

    tab_overview, tab_pollutants, tab_stations, tab_changes, tab_quality = st.tabs(
        ["National overview", "Pollutant analysis", "Station inventory", "Snapshot changes", "Data quality"]
    )

    with tab_overview:
        st.subheader("Latest national snapshot")
        metric_cols = st.columns(5)
        metric_cols[0].metric("Observations", fmt_int(overview.get("total_observations")))
        metric_cols[1].metric("Cities", fmt_int(overview.get("city_count")))
        metric_cols[2].metric("Stations", fmt_int(overview.get("station_count")))
        metric_cols[3].metric("Pollutants", fmt_int(overview.get("pollutant_count")))
        metric_cols[4].metric("Usable measurements", fmt_pct(overview.get("usable_measurement_pct")))

        pollutant_df = query_df(city_pollutant_sql())
        pollutant_df = to_numeric(
            pollutant_df,
            ["station_observation_count", "usable_observation_count", "reporting_station_count",
             "usable_station_count", "measurement_availability_pct", "mean_station_avg_value",
             "highest_station_max_value", "lowest_station_min_value"],
        )
        left, right = st.columns([1.2, 1])
        with left:
            st.markdown("#### Average reported concentration by pollutant")
            chart_df = pollutant_df.groupby(["pollutant_id", "pollutant_name"], as_index=False).apply(
                lambda group: pd.Series({
                    "weighted_value_sum": (
                        group["mean_station_avg_value"].fillna(0)
                        * group["usable_station_count"].fillna(0)
                    ).sum(),
                    "usable_station_weight": group["usable_station_count"].fillna(0).sum(),
                }), include_groups=False
            ).reset_index(drop=True)
            chart_df["mean_station_avg_value"] = (
                chart_df["weighted_value_sum"] / chart_df["usable_station_weight"].replace(0, pd.NA)
            )
            chart_df = chart_df.dropna(subset=["mean_station_avg_value"])
            if not chart_df.empty:
                fig = px.bar(
                    chart_df, x="pollutant_id", y="mean_station_avg_value",
                    hover_name="pollutant_name", labels={"pollutant_id": "Pollutant", "mean_station_avg_value": "Weighted mean of station averages"},
                    color="pollutant_id",
                )
                fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=20, b=10))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No usable pollutant averages are available in this snapshot.")
        with right:
            st.markdown("#### Measurement availability")
            quality_summary = pollutant_df.groupby("pollutant_id", as_index=False).agg(
                usable=("usable_observation_count", "sum"),
                total=("station_observation_count", "sum"),
            )
            quality_summary["unusable"] = quality_summary["total"] - quality_summary["usable"]
            if not quality_summary.empty:
                fig = px.bar(
                    quality_summary, x="pollutant_id", y=["usable", "unusable"],
                    barmode="stack", labels={"value": "Records", "pollutant_id": "Pollutant", "variable": "Status"},
                )
                fig.update_layout(margin=dict(l=10, r=10, t=20, b=10), legend_title_text="")
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### City-level monitoring completeness")
        completeness_df = query_df(city_completeness_sql())
        completeness_df = to_numeric(completeness_df, ["stations_reporting", "pollutants_reported", "measurement_availability_pct"])
        if not completeness_df.empty:
            st.dataframe(
                completeness_df.sort_values("measurement_availability_pct", ascending=True),
                use_container_width=True, hide_index=True,
            )
            st.download_button("Download city completeness CSV", completeness_df.to_csv(index=False), "city_completeness.csv", "text/csv")
        else:
            st.info("No city completeness rows were returned.")

    with tab_pollutants:
        st.subheader("Pollutant-level city analysis")
        selected = query_df(city_pollutant_sql(state=selected_state, city=selected_city, pollutant=selected_pollutant))
        selected = to_numeric(selected, ["measurement_availability_pct", "mean_station_avg_value", "highest_station_max_value", "lowest_station_min_value", "reporting_station_count", "usable_station_count"])
        if selected.empty:
            st.info("No records match the selected filters.")
        else:
            top = selected.sort_values("mean_station_avg_value", ascending=False).head(25)
            fig = px.bar(
                top, x="city", y="mean_station_avg_value", color="pollutant_id",
                hover_data=["state", "reporting_station_count", "measurement_availability_pct"],
                labels={"city": "City", "mean_station_avg_value": "Mean of station average values"},
            )
            fig.update_layout(margin=dict(l=10, r=10, t=20, b=10), xaxis_tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(selected, use_container_width=True, hide_index=True)
            st.download_button("Download pollutant analysis CSV", selected.to_csv(index=False), "pollutant_analysis.csv", "text/csv")
        st.caption("These are source-reported pollutant concentration summaries. They are not official CPCB AQI values.")

    with tab_stations:
        st.subheader("CPCB monitoring station inventory")
        st.caption(
            "Station coordinates and monitoring coverage from the latest complete extraction. "
            "This view is fully local-rendered and does not use external map tiles or map APIs."
        )
        station_df = query_df(
            station_inventory_sql(
                state=selected_state,
                city=selected_city,
                pollutant=selected_pollutant,
            )
        )
        station_df = to_numeric(
            station_df,
            ["latitude", "longitude", "pollutant_records", "usable_measurements"],
        )
        station_df = station_df.dropna(subset=["latitude", "longitude"])

        if station_df.empty:
            st.info("No station coordinates are available for the selected filters.")
        else:
            total_records = float(station_df["pollutant_records"].sum())
            usable_records = float(station_df["usable_measurements"].sum())
            availability_pct = (
                100.0 * usable_records / total_records if total_records > 0 else 0.0
            )
            metric_cols = st.columns(3)
            metric_cols[0].metric("Stations", fmt_int(len(station_df)))
            metric_cols[1].metric("States / UTs", fmt_int(station_df["state"].nunique()))
            metric_cols[2].metric("Measurement availability", fmt_pct(availability_pct))

            state_summary = (
                station_df.groupby("state", as_index=False)
                .agg(station_count=("station_key", "nunique"))
                .sort_values("station_count", ascending=False)
                .head(15)
            )
            if not state_summary.empty:
                st.markdown("#### States with the most monitoring stations")
                fig = px.bar(
                    state_summary.sort_values("station_count", ascending=True),
                    x="station_count",
                    y="state",
                    orientation="h",
                    labels={"station_count": "Monitoring stations", "state": "State / UT"},
                    text="station_count",
                )
                fig.update_layout(
                    margin=dict(l=10, r=10, t=20, b=10),
                    yaxis_title=None,
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### Station inventory")
            display_columns = [
                "station_name", "country", "state", "city", "latitude", "longitude",
                "pollutant_records", "usable_measurements", "station_key",
            ]
            display_df = station_df[[c for c in display_columns if c in station_df.columns]]
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download station inventory CSV",
                display_df.to_csv(index=False),
                "station_inventory.csv",
                "text/csv",
            )
            st.caption(
                "Coordinates are reported by the source dataset. This inventory is not a map "
                "and does not calculate official CPCB AQI."
            )

    with tab_changes:
        st.subheader("Change between the two latest retained extractions")
        comparison = first_row(query_df(snapshot_comparison_totals_sql()))
        if not comparison or comparison.get("compared_entity_count") is None:
            st.info("Snapshot comparison is not available yet. At least two complete extractions are needed.")
        else:
            st.caption(
                f"Previous: {comparison.get('previous_extraction_id')} · "
                f"Latest: {comparison.get('latest_extraction_id')}"
            )
            cols = st.columns(5)
            cols[0].metric("Compared entities", fmt_int(comparison.get("compared_entity_count")))
            cols[1].metric("Changed", fmt_int(comparison.get("changed_entity_count")))
            cols[2].metric("New", fmt_int(comparison.get("new_entity_count")))
            cols[3].metric("Missing", fmt_int(comparison.get("missing_entity_count")))
            cols[4].metric("Unchanged", fmt_int(comparison.get("unchanged_entity_count")))

            change_df = query_df(snapshot_comparison_by_pollutant_sql())
            change_df = to_numeric(change_df, ["changed_entity_count", "new_entity_count", "missing_entity_count", "unchanged_entity_count", "availability_changed_entity_count"])
            if not change_df.empty:
                fig = px.bar(
                    change_df, x="pollutant_id",
                    y=["changed_entity_count", "new_entity_count", "missing_entity_count", "unchanged_entity_count"],
                    barmode="group", labels={"value": "Entities", "pollutant_id": "Pollutant", "variable": "Change type"},
                )
                fig.update_layout(margin=dict(l=10, r=10, t=20, b=10), legend_title_text="")
                st.plotly_chart(fig, use_container_width=True)
            status_options = ["All statuses", "CHANGED", "NEW_IN_LATEST", "MISSING_FROM_LATEST", "UNCHANGED"]
            chosen_status = st.selectbox("Comparison status", status_options)
            detail = query_df(snapshot_change_detail_sql(
                state=selected_state, city=selected_city, pollutant=selected_pollutant, status=chosen_status
            ))
            detail = to_numeric(detail, ["previous_avg_value", "latest_avg_value", "avg_value_delta", "avg_value_pct_change"])
            st.dataframe(detail, use_container_width=True, hide_index=True)
            st.download_button("Download snapshot comparison CSV", detail.to_csv(index=False), "snapshot_changes.csv", "text/csv")
            st.metric("Availability changes", fmt_int(comparison.get("availability_changed_entity_count")))
        st.caption("This compares the latest two available complete snapshots. It is not a continuous time series.")

    with tab_quality:
        st.subheader("Measurement and record quality")
        quality_df = query_df(measurement_quality_sql())
        quality_df = to_numeric(quality_df, ["record_count"])
        if not quality_df.empty:
            status_counts = quality_df.groupby("measurement_status", as_index=False)["record_count"].sum()
            cols = st.columns(max(1, len(status_counts)))
            for index, row in status_counts.iterrows():
                cols[index].metric(str(row["measurement_status"]), fmt_int(row["record_count"]))
            fig = px.bar(
                quality_df, x="record_quality_reason", y="record_count", color="measurement_status",
                labels={"record_quality_reason": "Quality reason", "record_count": "Records"},
            )
            fig.update_layout(margin=dict(l=10, r=10, t=20, b=10), xaxis_tickangle=-30)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(quality_df, use_container_width=True, hide_index=True)
            st.download_button("Download quality report CSV", quality_df.to_csv(index=False), "measurement_quality.csv", "text/csv")
        else:
            st.info("No quality summary rows were returned.")

    st.divider()
    st.markdown(
        "<p class='section-note'>Data source: CPCB real-time monitoring API via data.gov.in. "
        "The dashboard displays reported pollutant concentrations and extraction-level comparisons. "
        "It does not calculate or certify official CPCB AQI.</p>",
        unsafe_allow_html=True,
    )

except AthenaQueryError as exc:
    display_query_error(exc)
except Exception as exc:  # keep UI failures visible while retaining detailed logs in terminal
    st.exception(exc)
