#!/bin/bash
set -e

echo "=========================================="
echo " Flight Delay Predictor — Cluster Mode"
echo "=========================================="

SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-spark-master-svc}"

# ── 1. Wait for Spark Master (port 6066 REST + 7077 RPC) ──────────
echo "Waiting for Spark Master at ${SPARK_MASTER_HOST}..."
until nc -z "${SPARK_MASTER_HOST}" 7077 2>/dev/null && nc -z "${SPARK_MASTER_HOST}" 6066 2>/dev/null; do
  echo "  Spark Master not ready, retrying in 3s..."
  sleep 3
done
echo "Spark Master ready."

# ── 2. Wait for workers to register (give them 10s after master) ──
echo "Waiting 10s for workers to register..."
sleep 10

# ── 3. Submit in cluster mode ──────────────────────────────────────
echo "Submitting FlightDelayPredictor to Spark cluster..."
exec /opt/spark/bin/spark-submit \
  --master "spark://${SPARK_MASTER_HOST}:7077" \
  --deploy-mode cluster \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf "spark.rpc.askTimeout=120s" \
  --conf "spark.network.timeout=120s" \
  --conf "spark.driver.extraJavaOptions=-DKAFKA_HOST=${KAFKA_HOST} -DMINIO_HOST=${MINIO_HOST} -DCASSANDRA_HOST=${CASSANDRA_HOST} --add-opens=java.base/sun.util.calendar=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=-DKAFKA_HOST=${KAFKA_HOST} -DMINIO_HOST=${MINIO_HOST} -DCASSANDRA_HOST=${CASSANDRA_HOST}" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://${MINIO_HOST}:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minio" \
  --conf "spark.hadoop.fs.s3a.secret.key=minio123" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.sql.ansi.enabled=false" \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
  http://jar-server:8888/flight_prediction_2.13-0.1.jar
