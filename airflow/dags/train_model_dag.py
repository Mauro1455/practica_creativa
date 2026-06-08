from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='train_flight_delay_model',
    description='Trains the Random Forest flight delay model on K8s Spark cluster (cluster mode)',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['spark', 'training', 'mlflow', 'k8s'],
) as dag:

    train = BashOperator(
        task_id='spark_submit_training',
        execution_timeout=None,
        bash_command="""
        set -e

        # Project ID desde el metadata server de GCP (no hardcodeado)
        PROJECT_ID=$(curl -sf --max-time 3 \
          "http://metadata.google.internal/computeMetadata/v1/project/project-id" \
          -H "Metadata-Flavor: Google" 2>/dev/null || \
          python3 -c "
import yaml, re
cfg = yaml.safe_load(open('/root/.kube/config'))
m = re.match(r'gke_([^_]+)_', cfg['clusters'][0]['name'])
print(m.group(1) if m else '')
" 2>/dev/null)

        if [[ -z "$PROJECT_ID" ]]; then
          echo "ERROR: No se pudo obtener el project ID de GCP"
          exit 1
        fi

        REGION="europe-west1"
        REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/practica"

        # K8s API server from mounted kubeconfig
        K8S_API=$(python3 -c "
import yaml
cfg = yaml.safe_load(open('/root/.kube/config'))
print(cfg['clusters'][0]['cluster']['server'])
")

        # VM public IP so the K8s driver pod can reach MinIO and MLflow
        VM_IP=$(curl -sf --max-time 3 \
          "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/accessConfigs/0/externalIp" \
          -H "Metadata-Flavor: Google" 2>/dev/null \
          || curl -sf --max-time 5 ifconfig.me 2>/dev/null)

        export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

        echo "K8s API : ${K8S_API}"
        echo "VM IP   : ${VM_IP}"
        echo "Submitting Spark job to K8s in cluster mode..."

        /opt/spark/bin/spark-submit \
          --master "k8s://${K8S_API}" \
          --deploy-mode cluster \
          --name airflow-flight-delay-training \
          --conf "spark.kubernetes.container.image=${REGISTRY}/spark-base:4.1.1" \
          --conf "spark.kubernetes.container.image.pullPolicy=Always" \
          --conf "spark.kubernetes.container.image.pullSecrets=ar-pull-secret" \
          --conf "spark.kubernetes.namespace=practica" \
          --conf "spark.kubernetes.authenticate.driver.serviceAccountName=spark" \
          --conf "spark.executor.instances=2" \
          --conf "spark.driver.memory=1g" \
          --conf "spark.executor.memory=2g" \
          --conf "spark.pyspark.python=python3" \
          --conf "spark.jars=local:///opt/spark/extra-jars/hadoop-aws-3.4.1.jar,local:///opt/spark/extra-jars/bundle-2.25.16.jar" \
          --conf "spark.kubernetes.driverEnv.MINIO_ENDPOINT=http://${VM_IP}:9000" \
          --conf "spark.kubernetes.driverEnv.MLFLOW_TRACKING_URI=http://${VM_IP}:5000" \
          --conf "spark.kubernetes.driverEnv.MLFLOW_S3_ENDPOINT_URL=http://${VM_IP}:9000" \
          --conf "spark.kubernetes.driverEnv.AWS_ACCESS_KEY_ID=minio" \
          --conf "spark.kubernetes.driverEnv.AWS_SECRET_ACCESS_KEY=minio123" \
          --conf "spark.executorEnv.MINIO_ENDPOINT=http://${VM_IP}:9000" \
          --conf "spark.hadoop.fs.s3a.endpoint=http://${VM_IP}:9000" \
          --conf "spark.hadoop.fs.s3a.access.key=minio" \
          --conf "spark.hadoop.fs.s3a.secret.key=minio123" \
          --conf "spark.hadoop.fs.s3a.path.style.access=true" \
          --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
          --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
          --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
          --conf "spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog" \
          --conf "spark.sql.catalog.lakehouse.type=hadoop" \
          --conf "spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse" \
          --packages "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0" \
          "local:///opt/spark/training/train_spark_mllib_model.py"
        """,
    )
