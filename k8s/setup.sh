#!/bin/bash
# setup.sh — Builds images, pushes to Artifact Registry, seeds data, and deploys
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
echo " Practica Creativa — Kubernetes Deploy"
echo "=============================================="
echo " Project:   $PROJECT_ID"
echo " Region:    $REGION"
echo " Registry:  $REGISTRY"
echo " Namespace: $NAMESPACE"
echo "=============================================="
echo ""

# ── 1. Enable APIs and create Artifact Registry repo ──────────────────────────
echo "[1/8] Setting up Artifact Registry..."
echo "Artifact Registry already enabled" --project="$PROJECT_ID" --quiet
gcloud artifacts repositories describe "$AR_REPO" \
  --location="$REGION" --project="$PROJECT_ID" > /dev/null 2>&1 || \
echo "Skipping repo creation - already exists"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ── 2. Build and push spark-base ───────────────────────────────────────────────
echo ""
echo "[2/8] Building spark-base image (this may take a few minutes)..."
# Copy Spark distribution to build context, excluding the large work/ directory
BUILD_CTX=$(mktemp -d)
trap "rm -rf $BUILD_CTX" EXIT

echo "  Copying spark-4.1.1 to build context (excluding work/ and extra-jars/)..."
rsync -a --exclude='work/' --exclude='extra-jars/' \
  "$PROJECT_ROOT/spark-4.1.1/" \
  "$BUILD_CTX/spark-4.1.1/"

# Copy extra-jars directly into jars/ so Docker sees them in a single COPY layer.
# (Only relevant if extra-jars/ was used; prepare.sh puts JARs directly in jars/)
if [ -d "$PROJECT_ROOT/spark-4.1.1/extra-jars" ]; then
  echo "  Merging extra-jars into build context jars/ (hadoop-aws + bundle)..."
  cp "$PROJECT_ROOT/spark-4.1.1/extra-jars/"*.jar "$BUILD_CTX/spark-4.1.1/jars/"
fi

cp k8s/Dockerfile.spark-base "$BUILD_CTX/Dockerfile"

docker build -t "${REGISTRY}/spark-base:4.1.1" "$BUILD_CTX"
docker push "${REGISTRY}/spark-base:4.1.1"
echo "  spark-base pushed."

# ── 3. Build and push spark-predictor ─────────────────────────────────────────
echo ""
echo "[3/8] Building spark-predictor image..."
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
echo "[4/8] Building flask image..."
docker build \
  -t "${REGISTRY}/flask:latest" \
  -f resources/web/Dockerfile.flask \
  resources/web/
docker push "${REGISTRY}/flask:latest"
echo "  flask pushed."

# ── 5. Apply manifests in order ────────────────────────────────────────────────
echo ""
echo "[5/8] Applying Kubernetes manifests..."

apply_manifest() {
  local file="$1"
  kubectl apply -f "$file"
}

kubectl apply -f k8s/00-namespace.yaml

# --server-side (SSA) avoids writing last-applied-configuration annotation,
# so files up to 1 MB per key are accepted (annotation limit is only 262 KB).
echo "  Creating data ConfigMaps..."
kubectl create configmap cassandra-init-data \
  --from-file=origin_dest_distances.jsonl=data/origin_dest_distances.jsonl \
  -n "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

kubectl create configmap spark-jar \
  --from-file=flight_prediction_2.13-0.1.jar=flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar \
  -n "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

echo "  Applying service manifests..."
apply_manifest k8s/01-kafka.yaml
apply_manifest k8s/02-kafka-init-job.yaml
apply_manifest k8s/03-cassandra.yaml
apply_manifest k8s/04-cassandra-init-job.yaml
apply_manifest k8s/05-minio.yaml
apply_manifest k8s/06-spark.yaml
apply_manifest k8s/07-jar-server.yaml
apply_manifest k8s/09-flask.yaml

# ── 6. Seed MinIO with models ──────────────────────────────────────────────────
echo ""
echo "[6/8] Waiting for MinIO to be ready..."
kubectl rollout status deployment/minio -n "$NAMESPACE" --timeout=120s

echo "  Seeding MinIO with models and jars..."
# Port-forward MinIO to localhost for mc access
kubectl port-forward -n "$NAMESPACE" svc/minio 9000:9000 &
PF_PID=$!
trap "kill $PF_PID 2>/dev/null; rm -rf $BUILD_CTX $PRED_CTX" EXIT
sleep 5

# Install mc if not present (write to /tmp to avoid needing sudo)
MC_BIN="${MC_BIN:-/tmp/mc}"
if ! command -v mc &>/dev/null && [[ ! -x "$MC_BIN" ]]; then
  echo "  Installing MinIO client (mc)..."
  curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o "$MC_BIN" \
    && chmod +x "$MC_BIN"
fi
MC="$MC_BIN"
[[ -x "$MC" ]] || MC=$(command -v mc 2>/dev/null || echo "/tmp/mc")

"$MC" alias set k8sminio http://localhost:9000 minio minio123 --quiet

# Create bucket
"$MC" mb --ignore-existing k8sminio/lakehouse

# Upload Spark ML models
echo "  Uploading models to MinIO (lakehouse/models/)..."
"$MC" cp --recursive models/ k8sminio/lakehouse/models/

# Upload the JAR to lakehouse/jars/ (some Spark jobs may read it from there)
echo "  Uploading JAR to MinIO (lakehouse/jars/)..."
"$MC" cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar \
   k8sminio/lakehouse/jars/

kill $PF_PID 2>/dev/null || true

echo "  MinIO seeded successfully."

# ── 7. Wait for Cassandra and Kafka init jobs ──────────────────────────────────
echo ""
echo "[7/8] Waiting for init jobs to complete..."
echo "  (Cassandra takes ~2 min to start; the job waits automatically)"
kubectl wait --for=condition=complete job/cassandra-init \
  -n "$NAMESPACE" --timeout=600s || {
  echo "WARNING: cassandra-init job did not complete in time."
  echo "  Check: kubectl logs -n $NAMESPACE job/cassandra-init"
}
kubectl wait --for=condition=complete job/kafka-init \
  -n "$NAMESPACE" --timeout=120s || {
  echo "WARNING: kafka-init job did not complete in time."
  echo "  Check: kubectl logs -n $NAMESPACE job/kafka-init"
}

# ── 8. Submit the Spark predictor job ─────────────────────────────────────────
echo ""
echo "[8/8] Submitting Spark predictor job..."
echo "  Waiting for Spark Master and Workers to be ready..."
kubectl rollout status deployment/spark-master -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/spark-worker -n "$NAMESPACE" --timeout=180s

apply_manifest k8s/08-spark-predictor-job.yaml

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo " Deployment complete!"
echo "=============================================="
echo ""
echo "Waiting for Flask LoadBalancer IP..."
for i in $(seq 1 30); do
  FLASK_IP=$(kubectl get svc flask -n "$NAMESPACE" \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
  if [[ -n "$FLASK_IP" ]]; then
    echo " Flask UI: http://${FLASK_IP}"
    break
  fi
  echo "  (${i}/30) Still provisioning LoadBalancer..."
  sleep 10
done

echo ""
echo "Useful commands:"
echo "  kubectl get all -n $NAMESPACE"
echo "  kubectl logs -n $NAMESPACE job/spark-predictor"
echo "  kubectl logs -n $NAMESPACE -l app=flask"
echo ""
echo "Spark Web UI (via port-forward):"
echo "  kubectl port-forward -n $NAMESPACE svc/spark-master-svc 8080:8080"
echo "  Then open http://localhost:8080"
echo ""
echo "MinIO Console (via port-forward):"
echo "  kubectl port-forward -n $NAMESPACE svc/minio 9001:9001"
echo "  Then open http://localhost:9001  (user: minio / password: minio123)"
