#!/bin/bash
# Entrena el modelo RandomForest en Spark K8s (--deploy-mode cluster).
# Requiere: GKE cluster ya creado (Paso 5) y kubeconfig configurado.
#
# Uso: bash train.sh
# Progreso: http://<IP-VM>:5000  (MLflow → Experiments)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROJECT_ID="${CLOUDSDK_CORE_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-europe-west1}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/practica"
NAMESPACE="practica"

# ── GKE API server ────────────────────────────────────────────────────────────
K8S_API=$(kubectl cluster-info 2>/dev/null \
  | grep -i 'control plane\|master' \
  | grep -oE 'https://[^ ]+' | head -1)

if [[ -z "$K8S_API" ]]; then
  echo "ERROR: kubectl no configurado. Ejecuta primero el Paso 5:"
  echo "  gcloud container clusters get-credentials practica-k8s --zone ${REGION}-b"
  exit 1
fi

# ── IP externa de la VM ───────────────────────────────────────────────────────
# MinIO (:9000) y MLflow (:5000) corren en Docker Compose en esta VM.
# Los pods de GKE los alcanzan por la IP pública (puerto abierto en Paso 1).
VM_IP=$(curl -sf --max-time 3 \
  "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/accessConfigs/0/externalIp" \
  -H "Metadata-Flavor: Google" 2>/dev/null \
  || curl -sf --max-time 5 ifconfig.me 2>/dev/null)

if [[ -z "$VM_IP" ]]; then
  echo "ERROR: No se pudo obtener la IP externa de la VM."
  echo "  Asegúrate de estar en la VM de GCloud (Paso 2)."
  exit 1
fi

echo "========================================================"
echo " Entrenando RandomForest — Spark K8s (cluster mode)"
echo "========================================================"
echo "  K8s API:  ${K8S_API}"
echo "  MinIO:    http://${VM_IP}:9000"
echo "  MLflow:   http://${VM_IP}:5000"
echo "  Imagen:   ${REGISTRY}/spark-base:4.1.1"
echo ""
echo "  Progreso en MLflow: http://${VM_IP}:5000 → Experiments"
echo ""

# ── Paso previo: datos + tabla Iceberg en MinIO ───────────────────────────────
# El entrenamiento lee de lakehouse.flights.training_features (tabla Iceberg en
# MinIO). Si no existe, la creamos aquí a partir del JSONL de entrenamiento.
TRAINING_DATA="${SCRIPT_DIR}/data/simple_flight_delay_features.jsonl.bz2"
MC="/tmp/mc"

# Descargar dataset si no está (gitignored; ~350 MB)
if [ ! -f "${TRAINING_DATA}" ]; then
  echo "[setup 1/2] Descargando dataset de entrenamiento (~350 MB)..."
  curl -fL "http://s3.amazonaws.com/agile_data_science/simple_flight_delay_features.jsonl.bz2" \
    -o "${TRAINING_DATA}"
  echo "  Dataset descargado."
fi

# Descargar mc si no está (cliente MinIO para comprobar si la tabla ya existe)
if [ ! -x "$MC" ]; then
  curl -fsSL "https://dl.min.io/client/mc/release/linux-amd64/mc" -o "$MC" && chmod +x "$MC"
fi
"$MC" alias set local-minio "http://localhost:9000" minio minio123 --quiet 2>/dev/null || true
"$MC" mb --ignore-existing local-minio/lakehouse &>/dev/null || true

TABLE_FILES=$("$MC" ls --recursive "local-minio/lakehouse/warehouse/flights/training_features/" 2>/dev/null | wc -l)
if [ "${TABLE_FILES}" -lt 1 ]; then
  echo "[setup 2/2] Creando tabla Iceberg en MinIO (primera vez, ~5-10 min)..."
  docker run --rm \
    --network host \
    -v "${TRAINING_DATA}:/practica/data/simple_flight_delay_features.jsonl.bz2:ro" \
    -v "${SCRIPT_DIR}/resources/setup_lakehouse.py:/practica/resources/setup_lakehouse.py:ro" \
    -v "${SCRIPT_DIR}/ivy2-cache:/root/.ivy2" \
    "${REGISTRY}/spark-base:4.1.1" \
    /opt/spark/bin/spark-submit \
      --master local[2] \
      --jars "/opt/spark/extra-jars/hadoop-aws-3.4.1.jar,/opt/spark/extra-jars/bundle-2.25.16.jar" \
      --packages "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0" \
      /practica/resources/setup_lakehouse.py /practica
  echo "[setup 2/2] Tabla Iceberg creada en MinIO."
else
  echo "[setup] Tabla Iceberg ya presente en MinIO."
fi
echo ""

# ── Kubeconfig sin exec-plugin ────────────────────────────────────────────────
# El contenedor eclipse-temurin no tiene gke-gcloud-auth-plugin, así que
# ~/.kube/config (que usa exec: ...) daría 401. Generamos un kubeconfig
# temporal con token Bearer estático que el cliente Fabric8 de Spark sí puede usar.
GKE_TOKEN=$(gcloud auth print-access-token)
K8S_CA_DATA=$(kubectl config view --raw \
  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
K8S_SERVER=$(kubectl config view --raw \
  -o jsonpath='{.clusters[0].cluster.server}')

SPARK_KUBECONFIG=$(mktemp /tmp/spark-kubeconfig-XXXX.yaml)
trap "rm -f ${SPARK_KUBECONFIG}" EXIT

cat > "${SPARK_KUBECONFIG}" <<KUBECFG
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: ${K8S_CA_DATA}
    server: ${K8S_SERVER}
  name: gke
contexts:
- context:
    cluster: gke
    user: spark-user
  name: gke
current-context: gke
users:
- name: spark-user
  user:
    token: ${GKE_TOKEN}
KUBECFG

echo "  Kubeconfig temporal: ${SPARK_KUBECONFIG}"

# spark-submit corre dentro de un contenedor eclipse-temurin (Java + Spark local).
# --network host: el contenedor usa la red de la VM.
# El driver y executors corren como pods de GKE y acceden al MinIO/MLflow
# de la VM via su IP pública (${VM_IP}).
docker run --rm \
  --network host \
  -v "${SPARK_KUBECONFIG}:/root/.kube/config:ro" \
  -v "${SCRIPT_DIR}/spark-4.1.1:/opt/spark:ro" \
  -v "${SCRIPT_DIR}/ivy2-cache:/root/.ivy2" \
  eclipse-temurin:17-jre \
  /opt/spark/bin/spark-submit \
    --master "k8s://${K8S_API}" \
    --deploy-mode cluster \
    --name flight-delay-training \
    --conf "spark.kubernetes.container.image=${REGISTRY}/spark-base:4.1.1" \
    --conf "spark.kubernetes.container.image.pullPolicy=Always" \
    --conf "spark.kubernetes.namespace=${NAMESPACE}" \
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

echo ""
echo "========================================================"
echo " Modelo entrenado y guardado en MinIO (bucket: lakehouse)"
echo " Continúa con: bash k8s/start_k8s.sh"
echo "========================================================"
