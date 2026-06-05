#!/usr/bin/env python3
"""
setup_lakehouse.py
Reads the raw training JSONL, converts it to an Iceberg table stored in MinIO.
Run this ONCE before training.

Usage:
    python3 resources/setup_lakehouse.py .
"""

import sys

def main(base_path="."):
    if not base_path:
        base_path = "."

    APP_NAME = "setup_lakehouse"

    import findspark
    findspark.init()
    import pyspark
    import pyspark.sql

    # ------------------------------------------------------------------ #
    # Spark session with Iceberg catalog + S3A (MinIO)                    #
    # ------------------------------------------------------------------ #
    spark = (
        pyspark.sql.SparkSession.builder
        .appName(APP_NAME)
        .master("local[*]")
        # --- Iceberg catalog pointing at MinIO ---
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse",
                "s3a://lakehouse/warehouse")
        # --- S3A / MinIO credentials ---
        .config("spark.hadoop.fs.s3a.endpoint",          "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key",        "minio")
        .config("spark.hadoop.fs.s3a.secret.key",        "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession created")

    # ------------------------------------------------------------------ #
    # Read raw data                                                        #
    # ------------------------------------------------------------------ #
    from pyspark.sql.types import (StringType, IntegerType, DoubleType,
                                   DateType, TimestampType,
                                   StructType, StructField)

    schema = StructType([
        StructField("ArrDelay",    DoubleType(),    True),
        StructField("CRSArrTime",  TimestampType(), True),
        StructField("CRSDepTime",  TimestampType(), True),
        StructField("Carrier",     StringType(),    True),
        StructField("DayOfMonth",  IntegerType(),   True),
        StructField("DayOfWeek",   IntegerType(),   True),
        StructField("DayOfYear",   IntegerType(),   True),
        StructField("DepDelay",    DoubleType(),    True),
        StructField("Dest",        StringType(),    True),
        StructField("Distance",    DoubleType(),    True),
        StructField("FlightDate",  DateType(),      True),
        StructField("FlightNum",   StringType(),    True),
        StructField("Origin",      StringType(),    True),
    ])

    input_path = "{}/data/simple_flight_delay_features.jsonl.bz2".format(base_path)
    print("📂 Reading raw data from: {}".format(input_path))
    features = spark.read.json(input_path, schema=schema)
    print("   Rows: {:,}".format(features.count()))

    # ------------------------------------------------------------------ #
    # Write as Iceberg table in MinIO                                      #
    # ------------------------------------------------------------------ #
    print("🧊 Writing Iceberg table  lakehouse.flights.training_features ...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.flights")

    (
        features.writeTo("lakehouse.flights.training_features")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )

    print("✅ Iceberg table written to s3a://lakehouse/warehouse/flights/training_features")
    print("   Verify at http://localhost:9001  bucket=lakehouse")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
