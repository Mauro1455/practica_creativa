#!/bin/bash
# Launches model training on YARN via a Docker container in the cluster network.
# Run from the project root: bash resources/train_on_yarn.sh
set -e

cd "$(dirname "$0")/.."

echo "Building spark-trainer image if needed..."
docker compose --profile training build spark-trainer

echo "Starting YARN training job..."
docker compose --profile training run --rm spark-trainer \
  /opt/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf "spark.driver.host=spark-trainer" \
    --conf "spark.yarn.jars=local:/opt/spark/jars/*" \
    --conf "spark.yarn.stagingDir=s3a://lakehouse/spark-staging" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    --conf "spark.hadoop.fs.s3a.access.key=minio" \
    --conf "spark.hadoop.fs.s3a.secret.key=minio123" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    /opt/project/resources/train_spark_mllib_model.py .
