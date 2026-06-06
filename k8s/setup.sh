#!/bin/bash
# setup.sh — Builds Docker images and pushes them to Artifact Registry.
# Called by start_k8s.sh; does NOT deploy manifests or seed MinIO.
# Run from the project root: bash k8s/setup.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID="${CLOUDSDK_CORE_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-europe-west1}"
AR_REPO="practica"
export REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}"
NAMESPACE="practica"

if [[ -z "$PROJECT_ID" ]]; then
  echo "ERROR: Could not determine GCP project. Run: gcloud config set project YOUR_PROJECT_ID"
  exit 1
fi

echo "=============================================="
echo " Practica Creativa — Build & Push Images"
echo "=============================================="
echo " Project:   $PROJECT_ID"
echo " Region:    $REGION"
echo " Registry:  $REGISTRY"
echo "=============================================="
echo ""

# ── 1. Artifact Registry auth ─────────────────────────────────────────────────
echo "[1/4] Authenticating Docker with Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
echo "  Auth configured."

# ── 2. Build and push spark-base ───────────────────────────────────────────────
echo ""
echo "[2/4] Building spark-base image..."
BUILD_CTX=$(mktemp -d)
trap "rm -rf $BUILD_CTX" EXIT

echo "  Copying spark-4.1.1 to build context (excluding work/)..."
# extra-jars/ is included: it goes to /opt/spark/extra-jars/ in the image.
# Spark Master/Workers do NOT auto-load extra-jars/ — only driver/executor load it
# via extraClassPath. This prevents the Netty conflict with SDK v2.
rsync -a --exclude='work/' \
  "$PROJECT_ROOT/spark-4.1.1/" \
  "$BUILD_CTX/spark-4.1.1/"

# Training script embedded for K8s Python cluster mode
mkdir -p "$BUILD_CTX/training"
cp "$PROJECT_ROOT/resources/train_spark_mllib_model.py" "$BUILD_CTX/training/"

cp k8s/Dockerfile.spark-base "$BUILD_CTX/Dockerfile"

docker build -t "${REGISTRY}/spark-base:4.1.1" "$BUILD_CTX"
docker push "${REGISTRY}/spark-base:4.1.1"
echo "  spark-base pushed."

# ── 3. Build and push spark-predictor ─────────────────────────────────────────
echo ""
echo "[3/4] Building spark-predictor image..."
PRED_CTX=$(mktemp -d)
trap "rm -rf $BUILD_CTX $PRED_CTX" EXIT

cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar "$PRED_CTX/"
cp flight_prediction/entrypoint.sh "$PRED_CTX/"
cp k8s/Dockerfile.spark-predictor "$PRED_CTX/Dockerfile"

docker build \
  --build-arg BASE_IMAGE="${REGISTRY}/spark-base:4.1.1" \
  -t "${REGISTRY}/spark-predictor:latest" \
  "$PRED_CTX"
docker push "${REGISTRY}/spark-predictor:latest"
echo "  spark-predictor pushed."

# ── 4. Build and push flask ────────────────────────────────────────────────────
echo ""
echo "[4/4] Building flask image..."
docker build \
  -t "${REGISTRY}/flask:latest" \
  -f resources/web/Dockerfile.flask \
  resources/web/
docker push "${REGISTRY}/flask:latest"
echo "  flask pushed."

echo ""
echo "=============================================="
echo " All images built and pushed to Artifact Registry."
echo " Next: apply.sh will deploy them to GKE."
echo "=============================================="
