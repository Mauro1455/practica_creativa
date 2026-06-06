#!/bin/bash
# start_k8s.sh — Arranque completo desde cero en GKE.
#
# Uso (desde la VM):
#   bash k8s/start_k8s.sh [--skip-build] [--skip-deploy] [--skip-models]
#
# Flags opcionales:
#   --skip-build   No reconstruye ni sube imágenes Docker (usar si ya están en Artifact Registry)
#   --skip-deploy  No aplica manifiestos K8s (usar si el cluster ya tiene todo desplegado)
#   --skip-models  No sube modelos ML a MinIO (usar si ya están subidos y son correctos)
#
# Requisitos:
#   - gcloud autenticado: gcloud auth login
#   - kubectl disponible: snap install kubectl --classic
#   - docker disponible: docker info
#   - Scala JAR compilado: sbt package en flight_prediction/
#   - Modelos ML en el MinIO local (docker compose MinIO): lakehouse/models/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ── Parse flags ────────────────────────────────────────────────────────────────
SKIP_BUILD=false
SKIP_DEPLOY=false
SKIP_MODELS=false
for arg in "$@"; do
  case "$arg" in
    --skip-build)  SKIP_BUILD=true  ;;
    --skip-deploy) SKIP_DEPLOY=true ;;
    --skip-models) SKIP_MODELS=true ;;
  esac
done

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID="${CLOUDSDK_CORE_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-europe-west1}"
ZONE="${ZONE:-europe-west1-b}"
CLUSTER_NAME="${CLUSTER_NAME:-practica-k8s}"
AR_REPO="practica"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}"
NAMESPACE="practica"
LOCAL_MINIO_URL="http://localhost:9000"   # Docker Compose MinIO on this VM
LOCAL_MINIO_USER="minio"
LOCAL_MINIO_PASS="minio123"
MC_BIN="${MC_BIN:-/tmp/mc}"

echo "=============================================="
echo " Practica Creativa — K8s Full Start"
echo "=============================================="
echo " Project:   ${PROJECT_ID}"
echo " Cluster:   ${CLUSTER_NAME} (${ZONE})"
echo " Registry:  ${REGISTRY}"
echo " Skip build:  ${SKIP_BUILD}"
echo " Skip deploy: ${SKIP_DEPLOY}"
echo " Skip models: ${SKIP_MODELS}"
echo "=============================================="
echo ""

# ── Prerequisites check ────────────────────────────────────────────────────────
echo "[pre] Checking prerequisites..."
command -v gcloud >/dev/null || { echo "ERROR: gcloud not found"; exit 1; }
command -v kubectl >/dev/null || { echo "ERROR: kubectl not found. Install: snap install kubectl --classic"; exit 1; }
command -v docker  >/dev/null || { echo "ERROR: docker not found"; exit 1; }

JAR_FILE="$PROJECT_ROOT/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"
DATA_FILE="$PROJECT_ROOT/data/origin_dest_distances.jsonl"
[[ -f "$JAR_FILE"  ]] || { echo "ERROR: JAR not found. Run: cd flight_prediction && sbt package"; exit 1; }
[[ -f "$DATA_FILE" ]] || { echo "ERROR: Data file not found: $DATA_FILE"; exit 1; }

echo "  JAR:  $JAR_FILE ($(wc -c < "$JAR_FILE") bytes)"
echo "  Data: $DATA_FILE ($(wc -c < "$DATA_FILE") bytes)"

# ── Step 0: Install mc if needed ──────────────────────────────────────────────
if ! command -v mc &>/dev/null && [[ ! -x "$MC_BIN" ]]; then
  echo ""
  echo "[0/7] Installing MinIO client (mc)..."
  curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o "$MC_BIN"
  chmod +x "$MC_BIN"
fi
MC="${MC_BIN}"
[[ -x "$MC" ]] || MC=$(command -v mc 2>/dev/null || echo "/tmp/mc")

# ── Step 1: Authenticate Docker with Artifact Registry ────────────────────────
echo ""
echo "[1/7] Authenticating Docker with Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
echo "  Docker auth configured."

# ── Step 2: Build and push Docker images ──────────────────────────────────────
if [[ "$SKIP_BUILD" == "false" ]]; then
  echo ""
  echo "[2/7] Building and pushing Docker images..."
  bash "$SCRIPT_DIR/setup.sh"
  echo "  Build and push complete."
else
  echo ""
  echo "[2/7] SKIPPED: Build and push (--skip-build)"
fi

# ── Step 3: Configure kubectl for GKE ─────────────────────────────────────────
echo ""
echo "[3/7] Configuring kubectl for GKE cluster ${CLUSTER_NAME}..."
export USE_GKE_GCLOUD_AUTH_PLUGIN=True
gcloud container clusters get-credentials "$CLUSTER_NAME" \
  --zone "$ZONE" --project "$PROJECT_ID" 2>/dev/null || true

# Fall back to token-based auth if plugin not available
if ! kubectl get ns "$NAMESPACE" &>/dev/null 2>&1; then
  echo "  gke-gcloud-auth-plugin not available, using token auth..."
  MASTER_IP=$(gcloud container clusters describe "$CLUSTER_NAME" \
    --zone "$ZONE" --project "$PROJECT_ID" \
    --format='value(endpoint)' 2>/dev/null)
  GKE_TOKEN=$(gcloud auth print-access-token 2>/dev/null)
  alias kubectl="kubectl --server=https://${MASTER_IP} --token=${GKE_TOKEN} --insecure-skip-tls-verify=true"
  export KUBECTL_OPTS="--server=https://${MASTER_IP} --token=${GKE_TOKEN} --insecure-skip-tls-verify=true"
  kubectl() { command kubectl $KUBECTL_OPTS "$@"; }
  export -f kubectl
fi

# Verify connectivity
if ! kubectl get ns "$NAMESPACE" &>/dev/null 2>&1; then
  kubectl get ns "$NAMESPACE" 2>/dev/null || true
  echo "  Namespace ${NAMESPACE} not found yet — deploy step will create it."
fi
echo "  kubectl connected to cluster."

# ── Step 4: Deploy K8s manifests ──────────────────────────────────────────────
if [[ "$SKIP_DEPLOY" == "false" ]]; then
  echo ""
  echo "[4/7] Deploying K8s manifests (apply.sh)..."
  bash "$SCRIPT_DIR/apply.sh"
  echo "  K8s manifests applied."
else
  echo ""
  echo "[4/7] SKIPPED: K8s deploy (--skip-deploy)"
fi

# ── Step 4b: Create/refresh Artifact Registry image pull secret ───────────────
# GKE nodes need this to pull images from AR when node scopes don't include
# cloud-platform. Token is valid ~1h; script refreshes it on every run.
echo ""
echo "[4b/7] Refreshing Artifact Registry image pull secret..."
kubectl create secret docker-registry ar-pull-secret \
  --docker-server="${REGION}-docker.pkg.dev" \
  --docker-username=oauth2accesstoken \
  --docker-password="$(gcloud auth print-access-token)" \
  -n "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply -f - 2>/dev/null || true
kubectl patch serviceaccount default -n "$NAMESPACE" \
  -p '{"imagePullSecrets": [{"name": "ar-pull-secret"}]}' 2>/dev/null || true
echo "  Pull secret refreshed — pods nuevos podrán hacer pull de AR."

# ── Step 5: Copy models from local MinIO to K8s MinIO via port-forward ────────
if [[ "$SKIP_MODELS" == "false" ]]; then
  echo ""
  echo "[5/7] Uploading ML models to K8s MinIO..."

  # K8s MinIO is ClusterIP (no external IP). Use port-forward to reach it.
  # Port 9002 avoids conflict with Docker Compose MinIO on 9000.
  echo "  Starting port-forward: localhost:9002 → K8s minio:9000..."
  kubectl port-forward -n "$NAMESPACE" svc/minio 9002:9000 &
  PF_PID=$!
  trap "kill $PF_PID 2>/dev/null || true" EXIT
  sleep 4

  K8S_MINIO_URL="http://localhost:9002"

  # Wait for MinIO to respond via port-forward
  echo "  Waiting for K8s MinIO to be ready..."
  for i in $(seq 1 20); do
    if curl -sf "${K8S_MINIO_URL}/minio/health/live" &>/dev/null; then
      echo "  MinIO is ready."
      break
    fi
    echo "  (${i}/20) MinIO not ready yet..."
    sleep 5
  done

  # Configure mc aliases
  echo "  Configuring mc aliases..."
  "$MC" alias set local-minio "$LOCAL_MINIO_URL" "$LOCAL_MINIO_USER" "$LOCAL_MINIO_PASS" --quiet
  "$MC" alias set k8s-minio   "$K8S_MINIO_URL"   "$LOCAL_MINIO_USER" "$LOCAL_MINIO_PASS" --quiet

  # Verify local MinIO has models
  if ! "$MC" ls local-minio/lakehouse/models/ &>/dev/null 2>&1; then
    echo "ERROR: Models not found in local MinIO (${LOCAL_MINIO_URL})."
    echo "  Make sure Docker Compose MinIO is running and models are uploaded."
    exit 1
  fi

  # Create bucket in K8s MinIO if needed
  "$MC" mb --ignore-existing k8s-minio/lakehouse &>/dev/null || true

  # CLEAN first to avoid duplicate parquet files (they corrupt model loading)
  echo "  Cleaning existing models in K8s MinIO (prevents duplicate file corruption)..."
  "$MC" rm --recursive --force k8s-minio/lakehouse/models/ &>/dev/null || true

  # Copy all models
  echo "  Copying models from local MinIO to K8s MinIO..."
  "$MC" cp --recursive local-minio/lakehouse/models/ k8s-minio/lakehouse/models/

  # Verify: RF model should have exactly 1 data parquet file
  RF_DATA_COUNT=$("$MC" ls k8s-minio/lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin/data/ 2>/dev/null | grep "\.parquet$" | wc -l)
  if [[ "$RF_DATA_COUNT" -ne 1 ]]; then
    echo "ERROR: RandomForest model data directory has ${RF_DATA_COUNT} parquet files (expected 1)."
    echo "  This will cause 'Decision Tree load failed' on Spark."
    echo "  Files:"
    "$MC" ls k8s-minio/lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin/data/
    exit 1
  fi

  echo "  Models uploaded successfully (RF data files: ${RF_DATA_COUNT})."
else
  echo ""
  echo "[5/7] SKIPPED: Model upload (--skip-models)"
fi

# ── Step 6: Restart spark-predictor job to pick up fresh models ───────────────
if [[ "$SKIP_MODELS" == "true" ]]; then
  echo ""
  echo "[6/7] SKIPPED: Predictor restart (--skip-models — no models yet, run train.sh first)"
else
  echo ""
  echo "[6/7] Restarting spark-predictor job with fresh models..."

  # Delete any existing job (it may have run with corrupt models)
  kubectl delete job spark-predictor -n "$NAMESPACE" --ignore-not-found=true
  sleep 3

  # Wait for spark master and workers to be ready
  echo "  Waiting for Spark Master ready..."
  kubectl rollout status deployment/spark-master -n "$NAMESPACE" --timeout=120s
  echo "  Waiting for Spark Workers ready..."
  kubectl rollout status deployment/spark-worker -n "$NAMESPACE" --timeout=120s

  # Apply the predictor job
  kubectl apply -f "$SCRIPT_DIR/08-spark-predictor-job.yaml"
  echo "  spark-predictor job created."
fi

# ── Step 7: Verification ──────────────────────────────────────────────────────
echo ""
echo "[7/7] Verifying deployment..."

if [[ "$SKIP_MODELS" == "true" ]]; then
  echo "  Predictor verification skipped (no models yet)."
  echo "  Next steps:"
  echo "    1. bash train.sh          — entrena el modelo en GKE (cluster mode)"
  echo "    2. bash k8s/start_k8s.sh --skip-build --skip-deploy  — copia modelos y arranca el predictor"
else
  # Wait for predictor job to complete (it just submits spark-submit and exits)
  echo "  Waiting for spark-predictor job to complete submission..."
  kubectl wait --for=condition=complete job/spark-predictor \
    -n "$NAMESPACE" --timeout=120s 2>/dev/null || {
      echo "  WARNING: spark-predictor job did not complete in 120s."
      echo "  Check: kubectl logs -n $NAMESPACE job/spark-predictor"
  }

  # Check spark-predictor logs for driver submission
  echo ""
  echo "  === spark-predictor job logs ==="
  kubectl logs -n "$NAMESPACE" job/spark-predictor 2>/dev/null | grep -E "Driver successfully submitted|RUNNING|Error|ERROR" | head -5

  # Wait for the Spark driver to start running on a worker (up to 3 minutes)
  echo ""
  echo "  Waiting for Spark driver to load models (may take 2-3 min)..."
  for i in $(seq 1 36); do
    DRIVER_STDOUT=$(kubectl exec -n "$NAMESPACE" -l app=spark-worker -- \
      sh -c 'tail -2 /var/lib/spark/work/driver-*/stdout 2>/dev/null | grep -v "==>"' 2>/dev/null | tail -3 || true)
    if echo "$DRIVER_STDOUT" | grep -q "Polling Kafka"; then
      echo "  Driver is RUNNING and polling Kafka!"
      break
    elif echo "$DRIVER_STDOUT" | grep -q "ERROR\|AssertionError\|Exception"; then
      echo "  WARNING: Driver encountered an error:"
      echo "  $DRIVER_STDOUT"
      break
    fi
    echo "  (${i}/36) Driver starting... $DRIVER_STDOUT"
    sleep 5
  done
fi

# Final status
echo ""
echo "=============================================="
echo " Verification Summary"
echo "=============================================="
kubectl get pods -n "$NAMESPACE" 2>/dev/null

echo ""
echo "  Flask external IP:"
FLASK_IP=$(kubectl get svc flask -n "$NAMESPACE" \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "pending")
echo "    http://${FLASK_IP}/flights/delays/predict_kafka"

echo ""
echo "  MinIO (models check):"
if [[ -n "${K8S_MINIO_URL:-}" ]]; then
  RF_COUNT=$("$MC" ls "k8s-minio/lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin/data/" 2>/dev/null | grep "\.parquet$" | wc -l || echo "?")
  echo "    RF model data files: ${RF_COUNT} (must be 1)"
fi

echo ""
echo "  Useful debug commands:"
echo "    kubectl get pods -n $NAMESPACE"
echo "    kubectl logs -n $NAMESPACE job/spark-predictor"
echo "    kubectl logs -n $NAMESPACE -l app=flask -f"
echo ""
echo "  Spark UI:   kubectl port-forward -n $NAMESPACE svc/spark-master-svc 8080:8080"
echo "  MinIO UI:   kubectl port-forward -n $NAMESPACE svc/minio 9001:9001"
echo ""
echo "=============================================="
echo " DONE — predictor should be running!"
echo "=============================================="
