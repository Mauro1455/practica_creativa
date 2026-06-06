#!/usr/bin/env python3
"""
train_spark_mllib_model.py  (Iceberg + MinIO version)
Reads training data from the Iceberg lakehouse on MinIO,
trains the Random Forest model and saves it back to MinIO.

Usage:
    python3 resources/train_spark_mllib_model.py .
"""

import sys, os, re
from os import environ
import mlflow
import mlflow.spark


def main(base_path="."):
    if not base_path:
        base_path = "."

    APP_NAME = "train_spark_mllib_model.py"

    try:
        sc and spark
    except (NameError, UnboundLocalError):
        import findspark
        findspark.init()
        import pyspark
        import pyspark.sql

        # ------------------------------------------------------------------ #
        # Spark session — Iceberg catalog + S3A (MinIO)                       #
        # ------------------------------------------------------------------ #
        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        spark = (
            pyspark.sql.SparkSession.builder
            .appName(APP_NAME)
            # master y deployMode vienen de spark-submit (--master k8s://... --deploy-mode cluster)
            # --- Iceberg ---
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hadoop")
            .config("spark.sql.catalog.lakehouse.warehouse",
                    "s3a://lakehouse/warehouse")
            # --- S3A / MinIO ---
            .config("spark.hadoop.fs.s3a.endpoint",          minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key",        "minio")
            .config("spark.hadoop.fs.s3a.secret.key",        "minio123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )
        sc = spark.sparkContext

    spark.sparkContext.setLogLevel("WARN")

    mlflow_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("flight_delay_prediction")

    # ------------------------------------------------------------------ #
    # Read features from Iceberg table (MinIO)                            #
    # ------------------------------------------------------------------ #
    print("🧊 Reading training data from Iceberg lakehouse ...")
    features = spark.table("lakehouse.flights.training_features")
    features.first()

    # ------------------------------------------------------------------ #
    # Check for nulls                                                      #
    # ------------------------------------------------------------------ #
    null_counts = [
        (col, features.where(features[col].isNull()).count())
        for col in features.columns
    ]
    cols_with_nulls = list(filter(lambda x: x[1] > 0, null_counts))
    print("Columns with nulls:", cols_with_nulls)

    # ------------------------------------------------------------------ #
    # Add Route column                                                     #
    # ------------------------------------------------------------------ #
    from pyspark.sql.functions import lit, concat
    features_with_route = features.withColumn(
        "Route",
        concat(features.Origin, lit("-"), features.Dest)
    )
    features_with_route.show(6)

    # ------------------------------------------------------------------ #
    # Bucketize ArrDelay                                                   #
    # ------------------------------------------------------------------ #
    from pyspark.ml.feature import Bucketizer

    splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
    arrival_bucketizer = Bucketizer(
        splits=splits,
        inputCol="ArrDelay",
        outputCol="ArrDelayBucket"
    )

    # Save bucketizer to MinIO
    arrival_bucketizer_path = "s3a://lakehouse/models/arrival_bucketizer_2.0.bin"
    arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)

    ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
    ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

    # ------------------------------------------------------------------ #
    # String indexers                                                      #
    # ------------------------------------------------------------------ #
    from pyspark.ml.feature import StringIndexer, VectorAssembler

    for column in ["Carrier", "Origin", "Dest", "Route"]:
        string_indexer = StringIndexer(inputCol=column, outputCol=column + "_index")
        string_indexer_model = string_indexer.fit(ml_bucketized_features)
        ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
        ml_bucketized_features = ml_bucketized_features.drop(column)

        # Save to MinIO
        path = "s3a://lakehouse/models/string_indexer_model_{}.bin".format(column)
        string_indexer_model.write().overwrite().save(path)

    # ------------------------------------------------------------------ #
    # Vector assembler                                                     #
    # ------------------------------------------------------------------ #
    numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
    index_columns   = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]

    vector_assembler = VectorAssembler(
        inputCols=numeric_columns + index_columns,
        outputCol="Features_vec"
    )
    final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

    # Save to MinIO
    vector_assembler_path = "s3a://lakehouse/models/numeric_vector_assembler.bin"
    vector_assembler.write().overwrite().save(vector_assembler_path)

    for column in index_columns:
        final_vectorized_features = final_vectorized_features.drop(column)

    final_vectorized_features.show()

    # ------------------------------------------------------------------ #
    # Train Random Forest                                                  #
    # ------------------------------------------------------------------ #
    from pyspark.ml.classification import RandomForestClassifier

    rfc = RandomForestClassifier(
        featuresCol="Features_vec",
        labelCol="ArrDelayBucket",
        predictionCol="Prediction",
        maxBins=4657,
        maxMemoryInMB=1024
    )

    model_output_path = "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"

    with mlflow.start_run(run_name="random_forest_training"):
        mlflow.log_param("maxBins", 4657)
        mlflow.log_param("maxMemoryInMB", 1024)
        mlflow.log_param("labelCol", "ArrDelayBucket")
        mlflow.log_param("splits", str([-float("inf"), -15.0, 0, 30.0, float("inf")]))

        model = rfc.fit(final_vectorized_features)

        model.write().overwrite().save(model_output_path)
        print("✅ Model saved to MinIO: {}".format(model_output_path))

        # ------------------------------------------------------------------ #
        # Evaluate                                                             #
        # ------------------------------------------------------------------ #
        predictions = model.transform(final_vectorized_features)

        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        evaluator = MulticlassClassificationEvaluator(
            predictionCol="Prediction",
            labelCol="ArrDelayBucket",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        print("Accuracy = {}".format(accuracy))

        mlflow.log_metric("accuracy", accuracy)

        mlflow.spark.log_model(model, "random_forest_model")
        print("✅ Model logged to MLflow")

    predictions.groupBy("Prediction").count().show()
    predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
