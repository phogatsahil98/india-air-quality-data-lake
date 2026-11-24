
import boto3

from botocore.exceptions import ClientError


# ==========================================
# 1. CONFIGURATION
# ==========================================

PROFILE = "india-aqi-admin"
REGION = "ap-south-1"


# ==========================================
# 2. AWS SESSION
# ==========================================

session = boto3.Session(
    profile_name=PROFILE,
    region_name=REGION
)

sts = session.client("sts")

identity = sts.get_caller_identity()

account_id = identity["Account"]

bucket_name = (
    f"india-aqi-lake-{account_id}-{REGION}"
)

s3 = session.client(
    "s3",
    region_name=REGION
)


# ==========================================
# 3. CREATE BUCKET
# ==========================================

def create_bucket():

    print("\nCreating S3 bucket...")

    try:

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": REGION
            }
        )

        print("Bucket created successfully.")

    except ClientError as exc:

        code = exc.response["Error"]["Code"]

        if code == "BucketAlreadyOwnedByYou":

            print("Bucket already belongs to this account.")

        elif code == "BucketAlreadyExists":

            raise RuntimeError(
                "Bucket name is already owned by another account."
            )

        else:

            raise

    # Verify region
    location = s3.get_bucket_location(
        Bucket=bucket_name
    )

    actual_region = (
        location.get("LocationConstraint")
        or "us-east-1"
    )

    if actual_region != REGION:

        raise RuntimeError(
            f"Wrong bucket region: {actual_region}"
        )

    print("Bucket region verified:", actual_region)


# ==========================================
# 4. BLOCK PUBLIC ACCESS
# ==========================================

def configure_public_access():

    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True
        }
    )

    print("Public access blocked.")


# ==========================================
# 5. ENFORCE BUCKET OWNERSHIP
# ==========================================

def configure_ownership():

    s3.put_bucket_ownership_controls(
        Bucket=bucket_name,
        OwnershipControls={
            "Rules": [
                {
                    "ObjectOwnership": "BucketOwnerEnforced"
                }
            ]
        }
    )

    print("Bucket ownership enforced.")


# ==========================================
# 6. ENABLE VERSIONING
# ==========================================

def enable_versioning():

    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
            "Status": "Enabled"
        }
    )

    print("Bucket versioning enabled.")


# ==========================================
# 7. ENABLE DEFAULT ENCRYPTION
# ==========================================

def enable_encryption():

    s3.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }
            ]
        }
    )

    print("Default SSE-S3 encryption enabled.")


# ==========================================
# 8. ADD PROJECT TAGS
# ==========================================

def add_tags():

    s3.put_bucket_tagging(
        Bucket=bucket_name,
        Tagging={
            "TagSet": [
                {
                    "Key": "Project",
                    "Value": "IndiaAirQualityDataLake"
                },
                {
                    "Key": "Environment",
                    "Value": "Learning"
                }
            ]
        }
    )

    print("Project tags applied.")


# ==========================================
# 9. VERIFY CONFIGURATION
# ==========================================

def verify_configuration():

    versioning = s3.get_bucket_versioning(
        Bucket=bucket_name
    )

    encryption = s3.get_bucket_encryption(
        Bucket=bucket_name
    )

    public_access = s3.get_public_access_block(
        Bucket=bucket_name
    )

    ownership = s3.get_bucket_ownership_controls(
        Bucket=bucket_name
    )

    print("\n" + "=" * 50)
    print("S3 BUCKET VERIFICATION")
    print("=" * 50)

    print("Bucket:", bucket_name)
    print("Region:", REGION)

    print(
        "Versioning:",
        versioning.get("Status")
    )

    print(
        "Encryption:",
        encryption[
            "ServerSideEncryptionConfiguration"
        ]["Rules"][0][
            "ApplyServerSideEncryptionByDefault"
        ]["SSEAlgorithm"]
    )

    print(
        "Public access block:",
        public_access["PublicAccessBlockConfiguration"]
    )

    print(
        "Object ownership:",
        ownership["OwnershipControls"]["Rules"][0][
            "ObjectOwnership"
        ]
    )

    print("\nS3 FOUNDATION READY!")


# ==========================================
# 10. MAIN
# ==========================================

if __name__ == "__main__":

    create_bucket()

    configure_public_access()

    configure_ownership()

    enable_versioning()

    enable_encryption()

    add_tags()

    verify_configuration()
