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
  echo "[1/4] Spark ya instalado en ./${SPARK_DIR}/"
else
  # Limpiar directorio vacío o incompleto si existe
  [ -d "$SPARK_DIR" ] && rm -rf "$SPARK_DIR"
  echo "[1/4] Descargando Spark ${SPARK_VERSION} (~280 MB)..."
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

# ── 2. JAR hadoop-aws (acceso S3A → MinIO) ────────────────────────────────────
HADOOP_AWS_JAR="hadoop-aws-3.4.1.jar"
if [ -f "${JARS_DIR}/${HADOOP_AWS_JAR}" ]; then
  echo "[2/4] ${HADOOP_AWS_JAR} ya presente."
else
  echo "[2/4] Descargando ${HADOOP_AWS_JAR} (~500 KB)..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/${HADOOP_AWS_JAR}" \
    -o "${JARS_DIR}/${HADOOP_AWS_JAR}"
  echo "  ${HADOOP_AWS_JAR} descargado."
fi

# ── 3. JAR aws-sdk-bundle v2 (credenciales S3A — Spark 4.x requiere SDK v2) ────
AWS_BUNDLE_JAR="bundle-2.25.16.jar"
if [ -f "${JARS_DIR}/${AWS_BUNDLE_JAR}" ]; then
  echo "[3/4] ${AWS_BUNDLE_JAR} ya presente."
else
  echo "[3/4] Descargando ${AWS_BUNDLE_JAR} (~450 MB — puede tardar varios minutos)..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/${AWS_BUNDLE_JAR}" \
    -o "${JARS_DIR}/${AWS_BUNDLE_JAR}"
  echo "  ${AWS_BUNDLE_JAR} descargado."
fi

# ── 4. JAR precompilado de Scala (evita instalar sbt) ─────────────────────────
TARGET_DIR="flight_prediction/target/scala-2.13"
JAR_NAME="flight_prediction_2.13-0.1.jar"
mkdir -p "$TARGET_DIR"
if [ -f "${TARGET_DIR}/${JAR_NAME}" ]; then
  echo "[4/4] JAR de Scala ya presente en ${TARGET_DIR}/"
elif [ -f "k8s/${JAR_NAME}" ]; then
  echo "[4/4] Copiando JAR precompilado desde k8s/..."
  cp "k8s/${JAR_NAME}" "${TARGET_DIR}/"
  echo "  JAR copiado a ${TARGET_DIR}/${JAR_NAME}"
else
  echo "[4/4] AVISO: No se encontró JAR precompilado."
  echo "  Compila manualmente: cd flight_prediction && sbt package"
fi

# ── Directorio cache de Ivy (evita re-descargas de paquetes Maven) ─────────────
mkdir -p ivy2-cache

echo ""
echo "========================================================"
echo " Preparación completada."
echo " Ahora puedes ejecutar: docker compose up --build -d"
echo "========================================================"
