
import boto3

from botocore.exceptions import ClientError


# ================================================
# 1. CONFIGURATION
# ================================================

PROFILE = "india-aqi-admin"
REGION = "ap-south-1"

DATABASE_NAME = "india_aqi_lake"
WORKGROUP_NAME = "india-aqi-workgroup"

QUERY_RESULTS_PREFIX = "athena-results/"

SCAN_LIMIT_BYTES = 256 * 1024 * 1024


# ================================================
# 2. AWS SESSION
# ================================================

session = boto3.Session(
    profile_name=PROFILE,
    region_name=REGION
)

identity = session.client("sts").get_caller_identity()

ACCOUNT_ID = identity["Account"]

BUCKET_NAME = (
    f"india-aqi-lake-{ACCOUNT_ID}-{REGION}"
)

BUCKET_URI = f"s3://{BUCKET_NAME}/"

QUERY_RESULTS_URI = (
    BUCKET_URI + QUERY_RESULTS_PREFIX
)

glue = session.client("glue", region_name=REGION)

athena = session.client("athena", region_name=REGION)


# ================================================
# 3. CREATE GLUE DATABASE
# ================================================

def create_glue_database():

    print("\nChecking Glue database...")

    try:

        response = glue.get_database(
            Name=DATABASE_NAME
        )

        database = response["Database"]

        print("Database already exists.")

    except glue.exceptions.EntityNotFoundException:

        glue.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,

                "Description": (
                    "Central metadata database for "
                    "India Air Quality Data Lake"
                ),

                "LocationUri": BUCKET_URI,

                "Parameters": {
                    "project": "IndiaAirQualityDataLake",
                    "environment": "learning",
                    "source": "CPCB"
                }
            }
        )

        print("Glue database created.")

        database = glue.get_database(
            Name=DATABASE_NAME
        )["Database"]

    print("Database name:", database["Name"])
    print("Database location:", database.get("LocationUri"))

    if database.get("LocationUri") != BUCKET_URI:

        raise RuntimeError(
            "Glue database location does not match "
            "the expected S3 bucket."
        )


# ================================================
# 4. CREATE ATHENA WORKGROUP
# ================================================

def create_athena_workgroup():

    print("\nChecking Athena workgroup...")

    existing_workgroups = []
    next_token = None

    while True:

        kwargs = {"MaxResults": 50}

        if next_token:
            kwargs["NextToken"] = next_token

        response = athena.list_work_groups(**kwargs)

        existing_workgroups.extend(
            response.get("WorkGroups", [])
        )

        next_token = response.get("NextToken")

        if not next_token:
            break

    exists = any(
        item["Name"] == WORKGROUP_NAME
        for item in existing_workgroups
    )

    if exists:

        print("Athena workgroup already exists.")

    else:

        athena.create_work_group(

            Name=WORKGROUP_NAME,

            Description=(
                "Athena workgroup for India Air Quality "
                "Data Lake analytics"
            ),

            Configuration={

                "ResultConfiguration": {

                    "OutputLocation": QUERY_RESULTS_URI,

                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_S3"
                    },

                    "ExpectedBucketOwner": ACCOUNT_ID
                },

                "EnforceWorkGroupConfiguration": True,

                "PublishCloudWatchMetricsEnabled": True,

                "BytesScannedCutoffPerQuery": (
                    SCAN_LIMIT_BYTES
                ),

                "RequesterPaysEnabled": False,

                "EngineVersion": {
                    "SelectedEngineVersion": (
                        "Athena engine version 3"
                    )
                }
            },

            Tags=[
                {
                    "Key": "Project",
                    "Value": "IndiaAirQualityDataLake"
                },
                {
                    "Key": "Environment",
                    "Value": "Learning"
                }
            ]
        )

        print("Athena workgroup created.")

    # Retrieve and verify actual configuration.

    response = athena.get_work_group(
        WorkGroup=WORKGROUP_NAME
    )

    workgroup = response["WorkGroup"]

    config = workgroup["Configuration"]

    result_config = config.get(
        "ResultConfiguration", {}
    )

    encryption = result_config.get(
        "EncryptionConfiguration", {}
    )

    print("\nATHENA WORKGROUP VERIFICATION")

    print("Name:", workgroup["Name"])
    print("State:", workgroup["State"])

    print(
        "Query results location:",
        result_config.get("OutputLocation")
    )

    print(
        "Result encryption:",
        encryption.get("EncryptionOption")
    )

    print(
        "Workgroup configuration enforced:",
        config.get("EnforceWorkGroupConfiguration")
    )

    print(
        "Bytes scanned cutoff:",
        config.get("BytesScannedCutoffPerQuery")
    )

    print(
        "Engine version:",
        config.get("EngineVersion", {}).get(
            "EffectiveEngineVersion"
        )
    )

    # Prevent silently accepting an incorrectly configured
    # pre-existing workgroup.

    if result_config.get("OutputLocation") != QUERY_RESULTS_URI:
        raise RuntimeError("Incorrect Athena results location.")

    if encryption.get("EncryptionOption") != "SSE_S3":
        raise RuntimeError("Athena result encryption is incorrect.")

    if not config.get("EnforceWorkGroupConfiguration"):
        raise RuntimeError("Workgroup settings are not enforced.")

    if config.get("BytesScannedCutoffPerQuery") != SCAN_LIMIT_BYTES:
        raise RuntimeError("Athena scan cutoff is incorrect.")

    if workgroup["State"] != "ENABLED":
        raise RuntimeError("Athena workgroup is not enabled.")

    print("\nAthena configuration verified.")


# ================================================
# 5. MAIN EXECUTION
# ================================================

if __name__ == "__main__":

    try:

        print("=" * 55)
        print("INDIA AQI - CATALOG AND QUERY SETUP")
        print("=" * 55)

        print("AWS Account verified:", bool(ACCOUNT_ID))
        print("Region:", REGION)
        print("S3 Bucket:", BUCKET_NAME)

        create_glue_database()

        create_athena_workgroup()

        print("\n" + "=" * 55)
        print("GLUE AND ATHENA FOUNDATION READY!")
        print("=" * 55)

    except ClientError as exc:

        error = exc.response["Error"]

        print("\nAWS ERROR:", error["Code"])
        print(error["Message"])

        raise SystemExit(1)

    except Exception as exc:

        print("\nSETUP ERROR:", str(exc))

        raise SystemExit(1)
