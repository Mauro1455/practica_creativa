#!/bin/bash
# Entrena el modelo RandomForest en el cluster Spark.
# Ejecutar con: bash train.sh
# Monitorizar progreso en MLflow: http://<IP-VM>:5000

set -e

echo "========================================================"
echo " Entrenando modelo RandomForest — Practica Creativa"
echo "========================================================"
echo ""
echo "  Progreso en MLflow: http://$(curl -s ifconfig.me 2>/dev/null || echo localhost):5000"
echo ""

docker exec airflow-scheduler bash -c "
  export MLFLOW_TRACKING_URI=http://mlflow:5000
  export MLFLOW_S3_ENDPOINT_URL=http://minio:9000
  export AWS_ACCESS_KEY_ID=minio
  export AWS_SECRET_ACCESS_KEY=minio123

  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode cluster \
    --conf spark.pyspark.python=python3 \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.25.16 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    /opt/project/resources/train_spark_mllib_model.py .
"

echo ""
echo "========================================================"
echo " Modelo entrenado y guardado en MinIO (bucket: lakehouse)"
echo "========================================================"
