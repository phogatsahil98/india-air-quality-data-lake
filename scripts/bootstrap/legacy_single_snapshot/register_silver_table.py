
import boto3
import sys

from pathlib import Path

from pyspark.sql import SparkSession
from botocore.exceptions import ClientError


# ==================================================
# 1. CONFIGURATION
# ==================================================

PROFILE = "india-aqi-admin"
REGION = "ap-south-1"

DATABASE_NAME = "india_aqi_lake"
TABLE_NAME = "cpcb_observations"

PARTITION_COLUMN = "ingestion_date"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

EXTRACTION_ID = "20261116T194631Z"

PARQUET_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "runs"
    / EXTRACTION_ID
    / "silver"
    / "cpcb_observations"
)


# ==================================================
# 2. AWS SESSION
# ==================================================

session = boto3.Session(
    profile_name=PROFILE,
    region_name=REGION
)

identity = session.client("sts").get_caller_identity()

ACCOUNT_ID = identity["Account"]

BUCKET_NAME = (
    f"india-aqi-lake-{ACCOUNT_ID}-{REGION}"
)

TABLE_LOCATION = (
    f"s3://{BUCKET_NAME}/silver/cpcb_observations/"
)


glue = session.client("glue", region_name=REGION)


# ==================================================
# 3. INITIALIZE SPARK
# ==================================================

def read_parquet_schema():

    if not PARQUET_DIR.exists():
        raise FileNotFoundError(PARQUET_DIR)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("IndiaAQI-Glue-Schema-Inspection")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:

        df = spark.read.parquet(str(PARQUET_DIR))

        print("\nACTUAL PARQUET SCHEMA")
        df.printSchema()

        if PARTITION_COLUMN not in df.columns:
            raise RuntimeError(
                "Partition column missing from Parquet discovery."
            )

        if df.count() != 3676:
            raise RuntimeError(
                "Unexpected Parquet record count."
            )

        # Exclude the partition column from physical
        # Parquet columns. Glue registers it separately.

        data_fields = [
            field
            for field in df.schema.fields
            if field.name != PARTITION_COLUMN
        ]

        schema = []

        for field in data_fields:

            column_type = field.dataType.simpleString()

            schema.append({
                "Name": field.name,
                "Type": column_type
            })

        partition_field = next(
            field
            for field in df.schema.fields
            if field.name == PARTITION_COLUMN
        )

        partition_type = partition_field.dataType.simpleString()

        return schema, partition_type

    finally:

        spark.stop()


# ==================================================
# 4. DEFINE GLUE STORAGE DESCRIPTOR
# ==================================================

def create_storage_descriptor(columns, location):

    return {

        "Columns": columns,

        "Location": location,

        "InputFormat": (
            "org.apache.hadoop.hive.ql.io.parquet."
            "MapredParquetInputFormat"
        ),

        "OutputFormat": (
            "org.apache.hadoop.hive.ql.io.parquet."
            "MapredParquetOutputFormat"
        ),

        "Compressed": True,

        "SerdeInfo": {

            "SerializationLibrary": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "serde.ParquetHiveSerDe"
            ),

            "Parameters": {
                "serialization.format": "1"
            }
        },

        "StoredAsSubDirectories": False
    }


# ==================================================
# 5. CREATE GLUE TABLE
# ==================================================

def create_glue_table(columns, partition_type):

    print("\nChecking Glue database...")

    glue.get_database(
        Name=DATABASE_NAME
    )

    table_input = {

        "Name": TABLE_NAME,

        "Description": (
            "Cleaned CPCB station-level pollutant observations "
            "produced by the Bronze-to-Silver PySpark pipeline."
        ),

        "TableType": "EXTERNAL_TABLE",

        "Parameters": {

            "EXTERNAL": "TRUE",

            "classification": "parquet",

            "compressionType": "snappy",

            "project": "IndiaAirQualityDataLake",

            "source": "CPCB"
        },

        "StorageDescriptor": create_storage_descriptor(
            columns,
            TABLE_LOCATION
        ),

        "PartitionKeys": [

            {
                "Name": PARTITION_COLUMN,
                "Type": partition_type
            }

        ]
    }

    print("\nChecking existing Glue table...")

    try:

        existing = glue.get_table(
            DatabaseName=DATABASE_NAME,
            Name=TABLE_NAME
        )["Table"]

        existing_columns = [
            (item["Name"], item["Type"])
            for item in existing["StorageDescriptor"]["Columns"]
        ]

        expected_columns = [
            (item["Name"], item["Type"])
            for item in columns
        ]

        existing_partitions = [
            (item["Name"], item["Type"])
            for item in existing.get("PartitionKeys", [])
        ]

        expected_partitions = [
            (PARTITION_COLUMN, partition_type)
        ]

        if existing_columns != expected_columns:
            raise RuntimeError(
                "Existing table schema differs from "
                "the actual Parquet schema."
            )

        if existing_partitions != expected_partitions:
            raise RuntimeError(
                "Existing Glue partition schema differs."
            )

        if existing["StorageDescriptor"]["Location"] != TABLE_LOCATION:
            raise RuntimeError(
                "Existing Glue table location differs."
            )

        print("Glue table already exists and schema matches.")

    except glue.exceptions.EntityNotFoundException:

        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput=table_input
        )

        print("Glue external table created successfully.")


# ==================================================
# 6. REGISTER S3 PARTITIONS
# ==================================================

def register_partitions(partition_type):

    print("\nDiscovering local partition folders...")

    partition_dirs = sorted(
        PARQUET_DIR.glob(f"{PARTITION_COLUMN}=*")
    )

    if not partition_dirs:
        raise RuntimeError(
            "No partition folders found."
        )

    created = 0
    existing_count = 0

    for partition_dir in partition_dirs:

        partition_value = partition_dir.name.split("=", 1)[1]

        # Validate the value before registration.

        from datetime import date

        date.fromisoformat(partition_value)

        partition_location = (
            TABLE_LOCATION
            + partition_dir.name
            + "/"
        )

        try:

            glue.get_partition(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                PartitionValues=[partition_value]
            )

            print(
                "Partition already registered:",
                partition_value
            )

            existing_count += 1

        except glue.exceptions.EntityNotFoundException:

            response = glue.get_table(
                DatabaseName=DATABASE_NAME,
                Name=TABLE_NAME
            )

            columns = response["Table"]["StorageDescriptor"]["Columns"]

            storage_descriptor = create_storage_descriptor(
                columns,
                partition_location
            )

            glue.create_partition(

                DatabaseName=DATABASE_NAME,

                TableName=TABLE_NAME,

                PartitionInput={

                    "Values": [
                        partition_value
                    ],

                    "StorageDescriptor": storage_descriptor,

                    "Parameters": {
                        "classification": "parquet"
                    }
                }
            )

            print(
                "Registered partition:",
                partition_value
            )

            created += 1

    print("\nPartitions created:", created)
    print("Existing partitions:", existing_count)


# ==================================================
# 7. FINAL VERIFICATION
# ==================================================

def verify_table():

    table = glue.get_table(
        DatabaseName=DATABASE_NAME,
        Name=TABLE_NAME
    )["Table"]

    partitions = glue.get_partitions(
        DatabaseName=DATABASE_NAME,
        TableName=TABLE_NAME
    )

    print("\n" + "=" * 55)
    print("GLUE TABLE VERIFICATION")
    print("=" * 55)

    print("Database:", DATABASE_NAME)
    print("Table:", TABLE_NAME)

    print(
        "Data columns:",
        len(table["StorageDescriptor"]["Columns"])
    )

    print(
        "Partition keys:",
        table["PartitionKeys"]
    )

    print(
        "S3 location:",
        table["StorageDescriptor"]["Location"]
    )

    print(
        "Registered partitions:",
        len(partitions.get("Partitions", []))
    )

    if len(partitions.get("Partitions", [])) == 0:
        raise RuntimeError("No Glue partitions registered.")

    print("\nGLUE REGISTRATION SUCCESSFUL!")


# ==================================================
# 8. MAIN
# ==================================================

if __name__ == "__main__":

    try:

        print("=" * 55)
        print("INDIA AQI - GLUE TABLE REGISTRATION")
        print("=" * 55)

        columns, partition_type = read_parquet_schema()

        print("\nDetected data columns:", len(columns))

        create_glue_table(columns, partition_type)

        register_partitions(partition_type)

        verify_table()

    except ClientError as exc:

        error = exc.response["Error"]

        print("\nAWS ERROR:", error["Code"])
        print(error["Message"])

        sys.exit(1)

    except Exception as exc:

        print("\nREGISTRATION FAILED:", str(exc))

        sys.exit(1)
