#!/bin/bash
set -e

echo "=========================================="
echo " Flight Delay Predictor — Cluster Mode"
echo "=========================================="

<<<<<<< HEAD
# ── 1. Wait for Spark Master REST server (port 6066) ──────────────
echo "Waiting for Spark Master REST server at spark-master:6066 ..."
until nc -z spark-master 6066 2>/dev/null; do
=======
# SPARK_MASTER_HOST defaults to "spark-master-svc" for Kubernetes.
# Override to "spark-master" when running with docker-compose.
SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-spark-master-svc}"

# ── 1. Wait for Spark Master REST server (port 6066) ──────────────
echo "Waiting for Spark Master REST server at ${SPARK_MASTER_HOST}:6066 ..."
until nc -z "${SPARK_MASTER_HOST}" 6066 2>/dev/null; do
>>>>>>> f053088 (Update files)
  echo "  Spark Master REST not ready, retrying in 3s..."
  sleep 3
done
echo "Spark Master REST ready."

<<<<<<< HEAD
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
=======
# ── 2. Cluster mode: driver runs on a Spark worker node ──────────
echo "Using cluster deploy mode — driver will run on a Spark worker."

# ── 3. Clear stale Kafka checkpoint ───────────────────────────────
echo "Clearing stale Kafka checkpoint from MinIO (if any)..."
mc alias set myminio http://${MINIO_HOST}:9000 minio minio123 --quiet 2>/dev/null || true
mc rm --recursive --force myminio/lakehouse/checkpoints/flight-predictor 2>/dev/null || true
echo "Checkpoint cleared (or did not exist)."

# ── 4. Submit in cluster mode ──────────────────────────────────────
# Key design decisions:
# - hadoop-aws-3.4.1 + bundle-2.24.6 (Netty stripped) are in /opt/spark/jars/
#   so S3AFileSystem lands in Hadoop's system classloader.
# - spark.executor.userClassPathFirst=true: fixes ClassCastException on
#   scala.collection.generic.DefaultSerializationProxy when KafkaWriter
#   serializes DataSourceRDDPartition.inputPartitions across classloaders.
# - startingOffsets=earliest: process all pending messages after restart.
echo "Submitting FlightDelayPredictor to Spark cluster..."
exec /opt/spark/bin/spark-submit \
  --master "spark://${SPARK_MASTER_HOST}:7077" \
  --deploy-mode cluster \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1" \
  --conf "spark.driver.extraJavaOptions=-DKAFKA_HOST=${KAFKA_HOST} -DMINIO_HOST=${MINIO_HOST} -DCASSANDRA_HOST=${CASSANDRA_HOST} --add-opens=java.base/sun.util.calendar=ALL-UNNAMED" \
>>>>>>> f053088 (Update files)
  --conf "spark.executor.extraJavaOptions=-DKAFKA_HOST=${KAFKA_HOST} -DMINIO_HOST=${MINIO_HOST} -DCASSANDRA_HOST=${CASSANDRA_HOST}" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://${MINIO_HOST}:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minio" \
  --conf "spark.hadoop.fs.s3a.secret.key=minio123" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
<<<<<<< HEAD
=======
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.sql.ansi.enabled=false" \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
>>>>>>> f053088 (Update files)
  http://jar-server:8888/flight_prediction_2.13-0.1.jar
