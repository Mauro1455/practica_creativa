#!/bin/bash
set -e

echo "=========================================="
echo " Flight Delay Predictor — Cluster Mode"
echo "=========================================="

# ── 1. Wait for Spark Master REST server (port 6066) ──────────────
echo "Waiting for Spark Master REST server at spark-master:6066 ..."
until nc -z spark-master 6066 2>/dev/null; do
  echo "  Spark Master REST not ready, retrying in 3s..."
  sleep 3
done
echo "Spark Master REST ready."

# ── 2. Submit in cluster mode ──────────────────────────────────────
# hadoop-aws + bundle go in extraClassPath (appended AFTER spark/jars/*)
# so Spark's Netty 4.2.7 takes priority over the bundle's Netty 4.1.
# S3AFileSystem lands in the system classloader → Hadoop finds it.
echo "Submitting FlightDelayPredictor to Spark cluster..."

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --supervise \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1" \
  --conf "spark.driver.extraClassPath=/opt/spark/extra-jars/hadoop-aws-3.4.1.jar:/opt/spark/extra-jars/bundle-2.24.6.jar" \
  --conf "spark.executor.extraClassPath=/opt/spark/extra-jars/hadoop-aws-3.4.1.jar:/opt/spark/extra-jars/bundle-2.24.6.jar" \
  --conf "spark.driver.extraJavaOptions=-DKAFKA_HOST=${KAFKA_HOST} -DMINIO_HOST=${MINIO_HOST} -DCASSANDRA_HOST=${CASSANDRA_HOST}" \
  --conf "spark.executor.extraJavaOptions=-DKAFKA_HOST=${KAFKA_HOST} -DMINIO_HOST=${MINIO_HOST} -DCASSANDRA_HOST=${CASSANDRA_HOST}" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://${MINIO_HOST}:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minio" \
  --conf "spark.hadoop.fs.s3a.secret.key=minio123" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  http://jar-server:8888/flight_prediction_2.13-0.1.jar
