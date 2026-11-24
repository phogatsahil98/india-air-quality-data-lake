
import boto3
from botocore.exceptions import ClientError

PROFILE = "india-aqi-admin"
REGION = "ap-south-1"


def main():

    print("Connecting to AWS...")

    try:
        session = boto3.Session(
            profile_name=PROFILE,
            region_name=REGION
        )

        sts = session.client("sts")

        identity = sts.get_caller_identity()

        print("\nAWS CONNECTION SUCCESSFUL")
        print("Account access verified.")
        print("Region:", REGION)

        print("\nCreating S3 client...")

        s3 = session.client("s3")

        response = s3.list_buckets()

        print("S3 connection successful!")
        print(
            "Existing buckets:",
            len(response.get("Buckets", []))
        )

    except ClientError as exc:

        print("\nAWS SERVICE ERROR:")
        print(exc.response["Error"]["Code"])
        print(exc.response["Error"]["Message"])

    except Exception as exc:

        print("\nCONNECTION ERROR:")
        print(type(exc).__name__)
        print(str(exc))


if __name__ == "__main__":
    main()
