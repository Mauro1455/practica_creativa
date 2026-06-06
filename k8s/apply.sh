#!/bin/bash
# apply.sh — Aplica los manifiestos en GKE desde Cloud Shell.
# Asume que las imágenes ya están construidas y subidas al Artifact Registry.
#
# Requiere en el directorio raíz del proyecto:
#   data/origin_dest_distances.jsonl
#   flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="practica"
MANIFESTS="$SCRIPT_DIR"

PROJECT_ID="${CLOUDSDK_CORE_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-europe-west1}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/practica"

# Applies a manifest substituting the registry placeholder with the current project registry
kube_apply() {
  sed "s|europe-west1-docker.pkg.dev/practicacreativa-497711/practica|${REGISTRY}|g" "$1" \
    | kubectl apply -f -
}

# ── Comprobaciones previas ────────────────────────────────────────────────────
DATA_FILE="$PROJECT_ROOT/data/origin_dest_distances.jsonl"
JAR_FILE="$PROJECT_ROOT/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"

if [[ ! -f "$DATA_FILE" ]]; then
  echo "ERROR: No se encuentra el fichero de distancias:"
  echo "  $DATA_FILE"
  exit 1
fi

if [[ ! -f "$JAR_FILE" ]]; then
  echo "ERROR: No se encuentra el JAR de Scala:"
  echo "  $JAR_FILE"
  echo "Compila con 'sbt package' desde flight_prediction/ antes de continuar."
  exit 1
fi

echo "Tamaño de los ficheros de datos:"
wc -c "$DATA_FILE" "$JAR_FILE"
echo ""

# ── 1. Namespace + RBAC ───────────────────────────────────────────────────────
echo "[1/8] Namespace y RBAC..."
kubectl apply -f "$MANIFESTS/00-namespace.yaml"
kubectl apply -f "$MANIFESTS/10-spark-rbac.yaml"

# ── 2. ConfigMaps de datos ────────────────────────────────────────────────────
# --server-side (SSA) guarda el estado en el servidor, NO escribe la anotación
# last-applied-configuration, por lo que no hay límite de 262 KB en la anotación.
# El único límite es el del dato en sí: 1 MB por clave de ConfigMap.
# origin_dest_distances.jsonl (~223 KB) y el JAR (<1 MB) caben holgadamente.
echo "[2/8] ConfigMaps (distancias + JAR)..."
kubectl create configmap cassandra-init-data \
  --from-file=origin_dest_distances.jsonl="$DATA_FILE" \
  -n "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

kubectl create configmap spark-jar \
  --from-file=flight_prediction_2.13-0.1.jar="$JAR_FILE" \
  -n "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

# ── 3. Kafka ──────────────────────────────────────────────────────────────────
echo "[3/8] Kafka..."
kubectl apply -f "$MANIFESTS/01-kafka.yaml"
echo "  Esperando a Kafka StatefulSet..."
kubectl rollout status statefulset/kafka -n "$NAMESPACE" --timeout=180s
kubectl apply -f "$MANIFESTS/02-kafka-init-job.yaml"

# ── 4. Cassandra + Init Job ───────────────────────────────────────────────────
echo "[4/8] Cassandra..."
kubectl apply -f "$MANIFESTS/03-cassandra.yaml"
kubectl apply -f "$MANIFESTS/04-cassandra-init-job.yaml"

# ── 5. MinIO ──────────────────────────────────────────────────────────────────
echo "[5/8] MinIO..."
kubectl apply -f "$MANIFESTS/05-minio.yaml"

# ── 6. Spark (Master + Workers) + jar-server + Flask ─────────────────────────
echo "[6/8] Spark, jar-server y Flask..."
kube_apply "$MANIFESTS/06-spark.yaml"
kubectl apply -f "$MANIFESTS/07-jar-server.yaml"
kube_apply "$MANIFESTS/09-flask.yaml"

# ── 7. Esperar a que los jobs de init terminen ────────────────────────────────
echo "[7/8] Esperando a jobs de inicialización..."

echo "  kafka-init (timeout 120s)..."
kubectl wait --for=condition=complete job/kafka-init \
  -n "$NAMESPACE" --timeout=120s || {
  echo "WARNING: kafka-init no completó a tiempo."
  echo "  Revisa: kubectl logs -n $NAMESPACE job/kafka-init"
}

echo "  cassandra-init (timeout 600s — Cassandra tarda ~2 min en arrancar)..."
kubectl wait --for=condition=complete job/cassandra-init \
  -n "$NAMESPACE" --timeout=600s || {
  echo "WARNING: cassandra-init no completó a tiempo."
  echo "  Revisa: kubectl logs -n $NAMESPACE job/cassandra-init"
}

# ── 8. Spark Predictor Job ────────────────────────────────────────────────────
echo "[8/8] Spark Predictor Job..."
echo "  Esperando a Spark Master..."
kubectl rollout status deployment/spark-master -n "$NAMESPACE" --timeout=180s
echo "  Esperando a Spark Workers (2 réplicas)..."
kubectl rollout status deployment/spark-worker -n "$NAMESPACE" --timeout=180s
echo "  Esperando a jar-server..."
kubectl rollout status deployment/jar-server -n "$NAMESPACE" --timeout=60s

kube_apply "$MANIFESTS/08-spark-predictor-job.yaml"

# ── Resumen ───────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo " Despliegue completado."
echo "=============================================="
echo ""
echo "Esperando IP pública de Flask..."
for i in $(seq 1 30); do
  FLASK_IP=$(kubectl get svc flask -n "$NAMESPACE" \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
  if [[ -n "$FLASK_IP" ]]; then
    echo " Flask UI: http://${FLASK_IP}/flights/delays/predict_kafka"
    break
  fi
  echo "  (${i}/30) Provisionando LoadBalancer..."
  sleep 10
done

echo ""
echo "Comandos útiles:"
echo "  kubectl get all -n $NAMESPACE"
echo "  kubectl get pods -n $NAMESPACE -w"
echo "  kubectl logs -n $NAMESPACE job/spark-predictor -f"
echo "  kubectl logs -n $NAMESPACE -l app=flask -f"
echo ""
echo "Spark Web UI:"
echo "  kubectl port-forward -n $NAMESPACE svc/spark-master-svc 8080:8080"
echo "  Abre: http://localhost:8080"
echo ""
echo "MinIO Console:"
echo "  kubectl port-forward -n $NAMESPACE svc/minio 9001:9001"
echo "  Abre: http://localhost:9001  (minio / minio123)"
