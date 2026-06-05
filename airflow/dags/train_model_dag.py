from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='train_flight_delay_model',
    description='Trains the Random Forest flight delay model on Spark cluster',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['spark', 'training', 'mlflow'],
) as dag:

    train = BashOperator(
        task_id='spark_submit_training',
        bash_command="""
        export MLFLOW_TRACKING_URI=http://mlflow:5000
        export MLFLOW_S3_ENDPOINT_URL=http://minio:9000
        export AWS_ACCESS_KEY_ID=minio
        export AWS_SECRET_ACCESS_KEY=minio123

        python3 -m pip install mlflow boto3 findspark -q --break-system-packages 2>/dev/null || pip3 install mlflow boto3 findspark -q

        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.pyspark.python=python3 \
          --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
          --conf spark.hadoop.fs.s3a.access.key=minio \
          --conf spark.hadoop.fs.s3a.secret.key=minio123 \
          --conf spark.hadoop.fs.s3a.path.style.access=true \
          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
          --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
          /opt/project/resources/train_spark_mllib_model.py .
        """,
    )
