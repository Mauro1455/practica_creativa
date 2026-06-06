#!/bin/bash
# prepare.sh — Descarga Spark 4.1.1 y los JARs extra necesarios (S3A/MinIO).
# Ejecutar UNA VEZ antes de "docker compose up --build".
#
# Uso:
#   bash prepare.sh

set -e

SPARK_VERSION="4.1.1"
SPARK_DIR="spark-${SPARK_VERSION}"
SPARK_TGZ="${SPARK_DIR}-bin-hadoop3.tgz"
SPARK_URL="https://downloads.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
SPARK_URL_FALLBACK="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

echo "========================================================"
echo " Preparando entorno Docker Compose — Practica Creativa"
echo "========================================================"
echo ""

# ── 1. Descargar y extraer Spark ──────────────────────────────────────────────
if [ -f "${SPARK_DIR}/bin/spark-class" ]; then
  echo "[1/6] Spark ya instalado en ./${SPARK_DIR}/"
else
  [ -d "$SPARK_DIR" ] && rm -rf "$SPARK_DIR"
  echo "[1/6] Descargando Spark ${SPARK_VERSION} (~280 MB)..."
  if command -v wget &>/dev/null; then
    wget -q --show-progress "$SPARK_URL" -O "$SPARK_TGZ" 2>&1 || \
    wget -q --show-progress "$SPARK_URL_FALLBACK" -O "$SPARK_TGZ"
  else
    curl -L --progress-bar "$SPARK_URL" -o "$SPARK_TGZ" || \
    curl -L --progress-bar "$SPARK_URL_FALLBACK" -o "$SPARK_TGZ"
  fi
  echo "  Extrayendo..."
  tar -xzf "$SPARK_TGZ"
  mv "${SPARK_DIR}-bin-hadoop3" "$SPARK_DIR"
  rm "$SPARK_TGZ"
  echo "  Spark extraído en ./${SPARK_DIR}/"
fi

JARS_DIR="${SPARK_DIR}/jars"
# extra-jars/ is loaded ONLY by driver/executor via extraClassPath — NOT by Spark Master/Workers.
# This avoids the Netty conflict: SDK v2 bundles old Netty which conflicts with Spark's Netty
# when both are in jars/. With extraClassPath, Spark's (newer) Netty always loads first.
EXTRA_JARS_DIR="${SPARK_DIR}/extra-jars"
mkdir -p "$EXTRA_JARS_DIR"

# Remove any old incompatible JARs that may be left from previous installs
rm -f "${JARS_DIR}/hadoop-aws-3.3.6.jar" \
      "${JARS_DIR}/hadoop-aws-3.4.1.jar" \
      "${JARS_DIR}/aws-java-sdk-bundle-1.12.761.jar" \
      "${JARS_DIR}/bundle-2.25.16.jar" \
      "${JARS_DIR}/bundle-2.*.jar" 2>/dev/null || true

# ── 2. hadoop-aws-3.4.1 → extra-jars/ ────────────────────────────────────────
# Must match Spark 4.1.1's bundled hadoop-client-runtime-3.4.2 (uses "60s" duration format).
# 3.3.x cannot parse that format → NumberFormatException when initializing S3AFileSystem.
HADOOP_AWS_JAR="hadoop-aws-3.4.1.jar"
if [ -f "${EXTRA_JARS_DIR}/${HADOOP_AWS_JAR}" ]; then
  echo "[2/6] ${HADOOP_AWS_JAR} ya presente en extra-jars/."
else
  echo "[2/6] Descargando ${HADOOP_AWS_JAR} (~500 KB) → extra-jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/${HADOOP_AWS_JAR}" \
    -o "${EXTRA_JARS_DIR}/${HADOOP_AWS_JAR}"
  echo "  ${HADOOP_AWS_JAR} descargado."
fi

# ── 3. AWS SDK v2 bundle → extra-jars/ ───────────────────────────────────────
# hadoop-aws-3.4.x requires SDK v2 (software.amazon.awssdk.*).
# Placed in extra-jars/ (NOT jars/) so Spark Master/Workers don't load it:
#   - Spark's Netty (jars/) loads first → SDK v2's older Netty is ignored (same class name wins)
#   - Avoids NoSuchMethodError in PooledByteBufAllocator that crashed the Spark Master
AWS_SDK_V2_JAR="bundle-2.25.16.jar"
if [ -f "${EXTRA_JARS_DIR}/${AWS_SDK_V2_JAR}" ]; then
  echo "[3/6] ${AWS_SDK_V2_JAR} ya presente en extra-jars/."
else
  echo "[3/6] Descargando ${AWS_SDK_V2_JAR} (~490 MB — puede tardar unos minutos) → extra-jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.16/${AWS_SDK_V2_JAR}" \
    -o "${EXTRA_JARS_DIR}/${AWS_SDK_V2_JAR}"
  echo "  ${AWS_SDK_V2_JAR} descargado."
fi

# ── 4. Datos estáticos de distancias entre aeropuertos ───────────────────────
DISTANCES_FILE="data/origin_dest_distances.jsonl"
mkdir -p data
if [ -f "${DISTANCES_FILE}" ]; then
  echo "[4/6] ${DISTANCES_FILE} ya presente ($(wc -l < ${DISTANCES_FILE}) líneas)."
else
  echo "[4/6] Descargando origin_dest_distances.jsonl (~380 KB)..."
  curl -fsSL "http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl" \
    -o "${DISTANCES_FILE}"
  echo "  Descargado: $(wc -l < ${DISTANCES_FILE}) registros de distancia."
fi

# ── 5. JAR precompilado de Scala (evita instalar sbt) ─────────────────────────
TARGET_DIR="flight_prediction/target/scala-2.13"
JAR_NAME="flight_prediction_2.13-0.1.jar"
mkdir -p "$TARGET_DIR"
if [ -f "${TARGET_DIR}/${JAR_NAME}" ]; then
  echo "[5/6] JAR de Scala ya presente en ${TARGET_DIR}/"
elif [ -f "k8s/${JAR_NAME}" ]; then
  echo "[5/6] Copiando JAR precompilado desde k8s/..."
  cp "k8s/${JAR_NAME}" "${TARGET_DIR}/"
  echo "  JAR copiado a ${TARGET_DIR}/${JAR_NAME}"
else
  echo "[5/6] AVISO: No se encontró JAR precompilado."
  echo "  Compila manualmente: cd flight_prediction && sbt package"
fi

# ── 6. Cassandra Java Driver (shaded) → jars/ ────────────────────────────────
# The predictor app uses com.datastax.oss.driver (DataStax OSS Driver v4).
# Marked "provided" in build.sbt → must be on the worker classpath at runtime.
# Shaded JAR: all internal deps (Netty, Guava…) are relocated → no conflict with Spark.
# Placed in jars/ so it's auto-loaded in /opt/spark/jars/* on driver startup.
CASSANDRA_JAR="java-driver-core-shaded-4.18.1.jar"
if [ -f "${JARS_DIR}/${CASSANDRA_JAR}" ]; then
  echo "[6/6] ${CASSANDRA_JAR} ya presente en jars/."
else
  echo "[6/6] Descargando ${CASSANDRA_JAR} (~7 MB) → jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/cassandra/java-driver-core-shaded/4.18.1/${CASSANDRA_JAR}" \
    -o "${JARS_DIR}/${CASSANDRA_JAR}"
  echo "  ${CASSANDRA_JAR} descargado."
fi

# ── Directorio cache de Ivy (evita re-descargas de paquetes Maven) ─────────────
mkdir -p ivy2-cache

echo ""
echo "========================================================"
echo " Preparación completada."
echo " Ahora puedes ejecutar: docker compose up --build -d"
echo "========================================================"
