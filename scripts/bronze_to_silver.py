
import json
import sys

import shutil

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


# =========================================================
# 1. PROJECT CONFIGURATION
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

EXTRACTION_ID = "20261116T194631Z"

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / f"source_profile_{EXTRACTION_ID}"
)

RUN_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "runs"
    / EXTRACTION_ID
)

PARQUET_DIR = (
    RUN_DIR
    / "silver"
    / "cpcb_observations"
)

REPORT_DIR = RUN_DIR / "metadata"

SOURCE_COLUMNS = [
    "country",
    "state",
    "city",
    "station",
    "last_update",
    "latitude",
    "longitude",
    "pollutant_id",
    "min_value",
    "max_value",
    "avg_value",
]

MISSING_TOKENS = [
    "",
    "NA",
    "N/A",
    "NULL",
    "NONE",
]

NUMERIC_PATTERN = (
    r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$"
)


# =========================================================
# 2. INGESTION METADATA
# =========================================================

def get_ingestion_metadata():

    ingestion_datetime = datetime.strptime(
        EXTRACTION_ID,
        "%Y%m%dT%H%M%SZ"
    ).replace(tzinfo=timezone.utc)

    ingestion_timestamp = (
        ingestion_datetime.strftime("%Y-%m-%d %H:%M:%S")
    )

    ingestion_date = ingestion_datetime.strftime("%Y-%m-%d")

    return ingestion_timestamp, ingestion_date


# =========================================================
# 3. INITIALIZE SPARK
# =========================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("IndiaAQI-Bronze-to-Silver")

        # Consistent timestamp handling
        .config("spark.sql.session.timeZone", "UTC")

        # Parquet compression
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )

        # Reduce unnecessary local shuffle partitions
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        # Windows local filesystem commit compatibility
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2"
        )

        # Avoid speculative duplicate task execution
        .config(
            "spark.speculation",
            "false"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

# =========================================================
# 4. READ BRONZE JSON FILES
# =========================================================

def read_bronze_data(spark):

    print("\nReading Bronze data...")

    if not SOURCE_DIR.exists():
        raise FileNotFoundError(
            f"Source directory not found: {SOURCE_DIR}"
        )

    files = sorted(
        SOURCE_DIR.glob("page_offset_*.json")
    )

    if not files:
        raise RuntimeError("No Bronze JSON files found.")

    print("Files discovered:", len(files))

    # Every source file contains one JSON envelope.
    # multiLine=True reads each complete JSON document.

    envelopes = (
        spark.read
        .option("multiLine", "true")
        .json([str(file) for file in files])
    )

    if "records" not in envelopes.columns:
        raise RuntimeError("Expected records array not found.")

    # Explode nested records and capture source filename.

    expanded = envelopes.select(
        F.explode("records").alias("record"),
        F.input_file_name().alias("_source_file")
    )

    raw = expanded.select(
        *[
            F.col(f"record.{column}")
            .cast("string")
            .alias(column)
            for column in SOURCE_COLUMNS
        ],

        F.regexp_extract(
            F.col("_source_file"),
            r"(page_offset_\d+\.json)",
            1
        ).alias("source_page")
    )

    for column in SOURCE_COLUMNS:

        if column not in raw.columns:
            raise RuntimeError(
                f"Required source field missing: {column}"
            )

    raw_count = raw.count()

    print("Bronze records extracted:", raw_count)

    if raw_count == 0:
        raise RuntimeError("Bronze extraction returned no records.")

    return raw, raw_count


# =========================================================
# 5. RECORD FINGERPRINTING AND DEDUPLICATION
# =========================================================

def deduplicate_records(raw):

    print("\nCalculating record fingerprints...")

    # Hash every original source field.
    # Source page is deliberately excluded from the hash.

    canonical_record = F.to_json(
        F.struct(
            *[
                F.col(column).alias(column)
                for column in SOURCE_COLUMNS
            ]
        )
    )

    hashed = raw.withColumn(
        "record_hash",
        F.sha2(canonical_record, 256)
    )

    # Group exact record fingerprints while retaining
    # the source pages that contained each observation.

    aggregates = [
        F.first(column, ignorenulls=False).alias(column)
        for column in SOURCE_COLUMNS
    ]

    deduplicated = (
        hashed
        .groupBy("record_hash")
        .agg(
            *aggregates,

            F.count("*").alias(
                "source_occurrence_count"
            ),

            F.sort_array(
                F.collect_set("source_page")
            ).alias("source_pages")
        )
        .withColumn(
            "source_page_count",
            F.size("source_pages")
        )
        .withColumn(
            "duplicate_excess_count",
            F.col("source_occurrence_count") - 1
        )
    )

    deduplicated_count = deduplicated.count()

    print("Records after exact deduplication:", deduplicated_count)

    print(
        "Exact duplicate excess rows removed:",
        raw.count() - deduplicated_count
    )

    return deduplicated, deduplicated_count


# =========================================================
# 6. NUMERIC CONVERSION HELPERS
# =========================================================

def numeric_expressions(column_name):

    original = F.col(column_name).cast("string")

    token = F.upper(F.trim(original))

    is_missing = (
        original.isNull()
        | token.isin(*MISSING_TOKENS)
    )

    is_numeric = F.coalesce(
        token.rlike(NUMERIC_PATTERN),
        F.lit(False)
    )

    parsed_value = F.when(
        is_numeric,
        token.cast("double")
    ).otherwise(
        F.lit(None).cast("double")
    )

    return token, is_missing, is_numeric, parsed_value


# =========================================================
# 7. CLEAN AND TYPE-CONVERT RECORDS
# =========================================================

def transform_records(df):

    print("\nStarting data transformations...")

    # Preserve original measurement strings for auditing.

    df = (
        df
        .withColumn("last_update_raw", F.col("last_update"))
        .withColumn("latitude_raw", F.col("latitude"))
        .withColumn("longitude_raw", F.col("longitude"))
        .withColumn("min_value_raw", F.col("min_value"))
        .withColumn("max_value_raw", F.col("max_value"))
        .withColumn("avg_value_raw", F.col("avg_value"))
    )

    # Normalize descriptive fields without losing original city
    # and station names.

    df = (
        df
        .withColumn("country", F.trim("country"))
        .withColumn("state", F.trim("state"))
        .withColumn("city", F.trim("city"))
        .withColumn("station", F.trim("station"))
        .withColumn(
            "city_normalized",
            F.lower(
                F.regexp_replace(
                    F.trim("city"),
                    r"\s+",
                    " "
                )
            )
        )
        .withColumn(
            "pollutant_id",
            F.upper(F.trim("pollutant_id"))
        )
    )

    # -------------------------------------
    # Numeric measurements
    # -------------------------------------

    numeric_info = {}

    for column in [
        "min_value",
        "max_value",
        "avg_value"
    ]:

        token, missing, numeric, parsed = (
            numeric_expressions(f"{column}_raw")
        )

        df = (
            df
            .withColumn(f"_{column}_missing", missing)
            .withColumn(f"_{column}_numeric", numeric)
            .withColumn(column, parsed)
        )

        numeric_info[column] = (
            missing,
            numeric
        )

    min_missing = numeric_info["min_value"][0]
    max_missing = numeric_info["max_value"][0]
    avg_missing = numeric_info["avg_value"][0]

    min_numeric = numeric_info["min_value"][1]
    max_numeric = numeric_info["max_value"][1]
    avg_numeric = numeric_info["avg_value"][1]

    all_missing = (
        min_missing & max_missing & avg_missing
    )

    any_missing = (
        min_missing | max_missing | avg_missing
    )

    any_invalid = (
        (~min_missing & ~min_numeric)
        | (~max_missing & ~max_numeric)
        | (~avg_missing & ~avg_numeric)
    )

    df = df.withColumn(
        "measurement_status",

        F.when(all_missing, "UNAVAILABLE")

        .when(any_invalid, "INVALID")

        .when(any_missing, "PARTIAL")

        .otherwise("AVAILABLE")
    )

    # Validate min <= average <= max.
    # We flag inconsistencies but do not delete rows.

    consistent_range = (
        (F.col("min_value") <= F.col("avg_value"))
        & (F.col("avg_value") <= F.col("max_value"))
    )

    df = df.withColumn(
        "range_consistency_status",

        F.when(
            F.col("measurement_status") != "AVAILABLE",
            "NOT_EVALUATED"
        )

        .when(
            consistent_range,
            "CONSISTENT"
        )

        .otherwise("INCONSISTENT")
    )

    # -------------------------------------
    # Coordinate conversion and validation
    # -------------------------------------

    lat_token, lat_missing, lat_numeric, latitude = (
        numeric_expressions("latitude_raw")
    )

    lon_token, lon_missing, lon_numeric, longitude = (
        numeric_expressions("longitude_raw")
    )

    df = (
        df
        .withColumn("latitude", latitude)
        .withColumn("longitude", longitude)
    )

    valid_latitude = F.col("latitude").between(-90, 90)
    valid_longitude = F.col("longitude").between(-180, 180)

    df = df.withColumn(
        "coordinate_status",

        F.when(
            (~lat_missing & ~lat_numeric)
            | (~lon_missing & ~lon_numeric),
            "INVALID"
        )

        .when(
            lat_missing | lon_missing,
            "MISSING"
        )

        .when(
            (~valid_latitude) | (~valid_longitude),
            "INVALID"
        )

        .otherwise("VALID")
    )

    # -------------------------------------
    # Observation timestamp
    # -------------------------------------

    # We preserve the source timestamp as a string because
    # its timezone semantics have not yet been established.
    # The parsed date is used for date-level partitioning.

    parsed_observation_time = F.to_timestamp(
        F.col("last_update_raw"),
        "dd-MM-yyyy HH:mm:ss"
    )

    df = (
        df
        .withColumn(
            "observation_date",
            F.to_date(parsed_observation_time)
        )
        .withColumn(
            "timestamp_parse_status",

            F.when(
                parsed_observation_time.isNull(),
                "INVALID"
            ).otherwise("VALID")
        )
    )

    # -------------------------------------
    # Stable monitoring station identifier
    # -------------------------------------

    station_identity = F.to_json(
        F.struct(
            F.lower(F.trim("country")).alias("country"),
            F.lower(F.trim("state")).alias("state"),
            F.col("city_normalized").alias("city"),
            F.lower(F.trim("station")).alias("station")
        )
    )

    df = df.withColumn(
        "station_key",
        F.sha2(station_identity, 256)
    )

    # -------------------------------------
    # Ingestion metadata
    # -------------------------------------

    ingestion_timestamp, ingestion_date = (
        get_ingestion_metadata()
    )

    df = (
        df
        .withColumn(
            "extraction_id",
            F.lit(EXTRACTION_ID)
        )
        .withColumn(
            "ingestion_timestamp_utc",
            F.to_timestamp(
                F.lit(ingestion_timestamp),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .withColumn(
            "ingestion_date",
            F.to_date(F.lit(ingestion_date))
        )
    )

    print("Data transformations completed.")

    return df


# =========================================================
# 8. SELECT FINAL SILVER SCHEMA
# =========================================================

def select_silver_columns(df):

    return df.select(
        "country",
        "state",
        "city",
        "city_normalized",
        "station",
        "station_key",

        "last_update_raw",
        "observation_date",
        "timestamp_parse_status",

        "latitude",
        "longitude",
        "latitude_raw",
        "longitude_raw",
        "coordinate_status",

        "pollutant_id",

        "min_value",
        "max_value",
        "avg_value",

        "min_value_raw",
        "max_value_raw",
        "avg_value_raw",

        "measurement_status",
        "range_consistency_status",

        "record_hash",
        "source_pages",
        "source_page_count",
        "source_occurrence_count",
        "duplicate_excess_count",

        "extraction_id",
        "ingestion_timestamp_utc",
        "ingestion_date"
    )


# =========================================================
# 9. DATA QUALITY REPORT
# =========================================================

def generate_quality_report(
    silver,
    raw_count,
    deduplicated_count
):

    print("\nGenerating quality metrics...")

    status_counts = {
        row["measurement_status"]: row["count"]
        for row in (
            silver
            .groupBy("measurement_status")
            .count()
            .collect()
        )
    }

    coordinate_counts = {
        row["coordinate_status"]: row["count"]
        for row in (
            silver
            .groupBy("coordinate_status")
            .count()
            .collect()
        )
    }

    metrics = silver.agg(
        F.countDistinct("city_normalized").alias("cities"),
        F.countDistinct("station_key").alias("stations"),
        F.countDistinct("pollutant_id").alias("pollutants"),

        F.sum(
            F.when(
                F.col("range_consistency_status") == "INCONSISTENT",
                1
            ).otherwise(0)
        ).alias("inconsistent_readings"),

        F.sum(
            F.when(
                F.col("timestamp_parse_status") == "INVALID",
                1
            ).otherwise(0)
        ).alias("invalid_timestamps"),

        F.min("observation_date").alias("earliest_observation_date"),
        F.max("observation_date").alias("latest_observation_date")
    ).first()

    report = {

        "project": "India Air Quality Data Lake",

        "source": "CPCB via data.gov.in",

        "extraction_id": EXTRACTION_ID,

        "source_records_read": raw_count,

        "silver_records_written": deduplicated_count,

        "exact_duplicate_excess_rows_removed": (
            raw_count - deduplicated_count
        ),

        "distinct_cities": metrics["cities"],

        "distinct_stations": metrics["stations"],

        "distinct_pollutants": metrics["pollutants"],

        "measurement_status_counts": status_counts,

        "coordinate_status_counts": coordinate_counts,

        "inconsistent_readings": (
            metrics["inconsistent_readings"] or 0
        ),

        "invalid_timestamps": (
            metrics["invalid_timestamps"] or 0
        ),

        "earliest_observation_date": (
            str(metrics["earliest_observation_date"])
            if metrics["earliest_observation_date"]
            else None
        ),

        "latest_observation_date": (
            str(metrics["latest_observation_date"])
            if metrics["latest_observation_date"]
            else None
        )
    }

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    report_path = REPORT_DIR / "quality_report.json"

    with report_path.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            indent=2,
            default=str
        )

    print("\nQUALITY REPORT")

    print(
        json.dumps(
            report,
            indent=2,
            default=str
        )
    )

    return report


# =========================================================
# 10. WRITE PARTITIONED PARQUET
# =========================================================


def write_silver_data(silver):

    print("\nWriting Silver Parquet...")

    PARQUET_DIR.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # Remove only the incomplete local Silver output
    # from the previous failed execution.

    if PARQUET_DIR.exists():

        print("Removing previous incomplete output...")

        shutil.rmtree(PARQUET_DIR)

    # This snapshot contains only 3,676 rows.
    # A single output partition avoids unnecessary
    # concurrent filesystem commit operations locally.

    local_output = silver.coalesce(1)

    (
        local_output
        .write
        .mode("errorifexists")
        .option("compression", "snappy")
        .partitionBy("ingestion_date")
        .parquet(str(PARQUET_DIR))
    )

    print("Silver Parquet written successfully.")

    # Read-back verification.

    verification = spark.read.parquet(
        str(PARQUET_DIR)
    )

    verified_count = verification.count()

    print("Read-back record count:", verified_count)

    if verified_count != silver.count():

        raise RuntimeError(
            "Silver record count verification failed."
        )

    print("Parquet integrity check passed.")

    return verified_count


# =========================================================
# 11. MAIN EXECUTION
# =========================================================

def main():

    global spark

    print("=" * 60)
    print("INDIA AQI - BRONZE TO SILVER ETL")
    print("=" * 60)

    spark = create_spark_session()

    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)

    try:

        raw, raw_count = read_bronze_data(spark)

        deduplicated, deduplicated_count = (
            deduplicate_records(raw)
        )

        transformed = transform_records(
            deduplicated
        )

        silver = select_silver_columns(
            transformed
        )

        generate_quality_report(
            silver,
            raw_count,
            deduplicated_count
        )

        verified_count = write_silver_data(
            silver
        )

        if verified_count != deduplicated_count:

            raise RuntimeError(
                "Silver Parquet verification failed."
            )

        print("\n" + "=" * 60)
        print("SILVER ETL COMPLETED SUCCESSFULLY")
        print("=" * 60)

        print("Bronze records:", raw_count)
        print("Silver records:", verified_count)

        print(
            "Exact duplicate excess rows removed:",
            raw_count - verified_count
        )

        print("Parquet directory:", PARQUET_DIR)

        print(
            "Quality report:",
            REPORT_DIR / "quality_report.json"
        )

    finally:

        spark.stop()


if __name__ == "__main__":

    try:

        main()

    except Exception as exc:

        print("\nETL FAILED:", str(exc))

        raise SystemExit(1)
