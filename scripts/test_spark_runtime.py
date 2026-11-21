
import shutil
from pathlib import Path

from pyspark.sql import SparkSession


OUTPUT_PATH = Path(
    "data/tmp/spark_smoke_test"
)


def main():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("IndiaAQI-Spark-Smoke-Test")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:

        print("=" * 50)
        print("PYSPARK RUNTIME VERIFICATION")
        print("=" * 50)

        print("Spark version:", spark.version)

        print(
            "Spark master:",
            spark.sparkContext.master
        )

        # Create a small test DataFrame.
        df = spark.range(1, 6)

        print("\nTEST DATAFRAME:")
        df.show()

        # Test Parquet writing.
        if OUTPUT_PATH.exists():
            shutil.rmtree(OUTPUT_PATH)

        df.write.mode("overwrite").parquet(
            str(OUTPUT_PATH)
        )

        print("Parquet writing successful.")

        # Read Parquet back.
        result = spark.read.parquet(
            str(OUTPUT_PATH)
        )

        count = result.count()

        print("Parquet records:", count)

        assert count == 5, "Record count mismatch!"

        print("\nPYSPARK RUNTIME READY!")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
