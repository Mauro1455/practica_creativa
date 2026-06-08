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
  echo "[1/11] Spark ya instalado en ./${SPARK_DIR}/"
else
  [ -d "$SPARK_DIR" ] && rm -rf "$SPARK_DIR"
  echo "[1/11] Descargando Spark ${SPARK_VERSION} (~280 MB)..."
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
  echo "[2/11] ${HADOOP_AWS_JAR} ya presente en extra-jars/."
else
  echo "[2/11] Descargando ${HADOOP_AWS_JAR} (~500 KB) → extra-jars/..."
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
  echo "[3/11] ${AWS_SDK_V2_JAR} ya presente en extra-jars/."
else
  echo "[3/11] Descargando ${AWS_SDK_V2_JAR} (~490 MB — puede tardar unos minutos) → extra-jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.16/${AWS_SDK_V2_JAR}" \
    -o "${EXTRA_JARS_DIR}/${AWS_SDK_V2_JAR}"
  echo "  ${AWS_SDK_V2_JAR} descargado."
fi

# ── 4. Datos estáticos de distancias entre aeropuertos ───────────────────────
DISTANCES_FILE="data/origin_dest_distances.jsonl"
mkdir -p data
if [ -f "${DISTANCES_FILE}" ]; then
  echo "[4/11] ${DISTANCES_FILE} ya presente ($(wc -l < ${DISTANCES_FILE}) líneas)."
else
  echo "[4/11] Descargando origin_dest_distances.jsonl (~380 KB)..."
  curl -fsSL "http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl" \
    -o "${DISTANCES_FILE}"
  echo "  Descargado: $(wc -l < ${DISTANCES_FILE}) registros de distancia."
fi

# ── 5. JAR precompilado de Scala (evita instalar sbt) ─────────────────────────
TARGET_DIR="flight_prediction/target/scala-2.13"
JAR_NAME="flight_prediction_2.13-0.1.jar"
mkdir -p "$TARGET_DIR"
if [ -f "${TARGET_DIR}/${JAR_NAME}" ]; then
  echo "[5/11] JAR de Scala ya presente en ${TARGET_DIR}/"
elif [ -f "k8s/${JAR_NAME}" ]; then
  echo "[5/11] Copiando JAR precompilado desde k8s/..."
  cp "k8s/${JAR_NAME}" "${TARGET_DIR}/"
  echo "  JAR copiado a ${TARGET_DIR}/${JAR_NAME}"
else
  echo "[5/11] AVISO: No se encontró JAR precompilado."
  echo "  Compila manualmente: cd flight_prediction && sbt package"
fi

# ── 6. kafka-clients → jars/ ─────────────────────────────────────────────────
# El predictor usa KafkaConsumer/KafkaProducer directamente (sin Spark Streaming).
# spark-sql-kafka-0-10 está como "provided" en build.sbt pero NO viene en la
# distribución estándar de Spark → NoClassDefFoundError en el driver en cluster mode.
KAFKA_CLIENTS_JAR="kafka-clients-3.9.0.jar"
if [ -f "${JARS_DIR}/${KAFKA_CLIENTS_JAR}" ]; then
  echo "[6/11] ${KAFKA_CLIENTS_JAR} ya presente en jars/."
else
  echo "[6/11] Descargando ${KAFKA_CLIENTS_JAR} (~5 MB) → jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/${KAFKA_CLIENTS_JAR}" \
    -o "${JARS_DIR}/${KAFKA_CLIENTS_JAR}"
  echo "  ${KAFKA_CLIENTS_JAR} descargado."
fi

# ── 7. Cassandra Java Driver (shaded) → jars/ ────────────────────────────────
# The predictor app uses com.datastax.oss.driver (DataStax OSS Driver v4).
# Marked "provided" in build.sbt → must be on the worker classpath at runtime.
# Must use com.datastax.oss:java-driver-core-shaded (DataStax) instead of
# org.apache.cassandra:java-driver-core-shaded: the DataStax version bundles
# the shaded Guava (com.datastax.oss.driver.shaded.guava.*) inside the JAR,
# which the driver's bytecode references at runtime. The Apache version omits it.
CASSANDRA_JAR="java-driver-core-shaded-4.17.0.jar"
if [ -f "${JARS_DIR}/${CASSANDRA_JAR}" ]; then
  echo "[7/11] ${CASSANDRA_JAR} ya presente en jars/."
else
  echo "[7/11] Descargando ${CASSANDRA_JAR} (~11 MB) → jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core-shaded/4.17.0/${CASSANDRA_JAR}" \
    -o "${JARS_DIR}/${CASSANDRA_JAR}"
  echo "  ${CASSANDRA_JAR} descargado."
fi

# ── 8. Cassandra Shaded Guava → jars/ ────────────────────────────────────────
# java-driver-core-shaded does NOT bundle Guava inside the JAR; it lists
# java-driver-shaded-guava as a compile-scope dependency (see its POM).
# Without this JAR the driver throws NoClassDefFoundError on CqlSession.builder():
#   com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList
SHADED_GUAVA_JAR="java-driver-shaded-guava-25.1-jre-graal-sub-1.jar"
if [ -f "${JARS_DIR}/${SHADED_GUAVA_JAR}" ]; then
  echo "[8/11] ${SHADED_GUAVA_JAR} ya presente en jars/."
else
  echo "[8/11] Descargando ${SHADED_GUAVA_JAR} (~3 MB) → jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/25.1-jre-graal-sub-1/${SHADED_GUAVA_JAR}" \
    -o "${JARS_DIR}/${SHADED_GUAVA_JAR}"
  echo "  ${SHADED_GUAVA_JAR} descargado."
fi

# ── 9. Typesafe Config → jars/ ───────────────────────────────────────────────
# java-driver-core-shaded does NOT shade com.typesafe:config; it's an external
# dependency used for driver configuration (reference.conf / application.conf).
# Without it the driver throws NoClassDefFoundError on CqlSession.builder():
#   com.typesafe.config.ConfigMergeable
TYPESAFE_CONFIG_JAR="config-1.4.1.jar"
if [ -f "${JARS_DIR}/${TYPESAFE_CONFIG_JAR}" ]; then
  echo "[9/11] ${TYPESAFE_CONFIG_JAR} ya presente en jars/."
else
  echo "[9/11] Descargando ${TYPESAFE_CONFIG_JAR} (~300 KB) → jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/com/typesafe/config/1.4.1/${TYPESAFE_CONFIG_JAR}" \
    -o "${JARS_DIR}/${TYPESAFE_CONFIG_JAR}"
  echo "  ${TYPESAFE_CONFIG_JAR} descargado."
fi

# ── 10. DataStax native-protocol → jars/ ─────────────────────────────────────
# native-protocol is NOT shaded into java-driver-core-shaded; it's a direct
# dependency listed in the POM with compile scope (com.datastax.oss.protocol.*).
# Without it the driver throws NoClassDefFoundError on CqlSession.builder():
#   com.datastax.oss.protocol.internal.PrimitiveCodec
NATIVE_PROTOCOL_JAR="native-protocol-1.5.1.jar"
if [ -f "${JARS_DIR}/${NATIVE_PROTOCOL_JAR}" ]; then
  echo "[10/11] ${NATIVE_PROTOCOL_JAR} ya presente en jars/."
else
  echo "[10/11] Descargando ${NATIVE_PROTOCOL_JAR} (~313 KB) → jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/com/datastax/oss/native-protocol/1.5.1/${NATIVE_PROTOCOL_JAR}" \
    -o "${JARS_DIR}/${NATIVE_PROTOCOL_JAR}"
  echo "  ${NATIVE_PROTOCOL_JAR} descargado."
fi

# ── 11. jnr-posix → jars/ ────────────────────────────────────────────────────
# The DataStax driver tries to use Unix domain sockets via jnr-posix for faster
# local connections. Without this JAR it logs NoClassDefFoundError: POSIXHandler
# to stderr and falls back to TCP (non-fatal). Including it avoids the noise.
JNR_POSIX_JAR="jnr-posix-3.1.18.jar"
if [ -f "${JARS_DIR}/${JNR_POSIX_JAR}" ]; then
  echo "[11/11] ${JNR_POSIX_JAR} ya presente en jars/."
else
  echo "[11/11] Descargando ${JNR_POSIX_JAR} (~280 KB) → jars/..."
  curl -fsSL \
    "https://repo1.maven.org/maven2/com/github/jnr/jnr-posix/3.1.18/${JNR_POSIX_JAR}" \
    -o "${JARS_DIR}/${JNR_POSIX_JAR}"
  echo "  ${JNR_POSIX_JAR} descargado."
fi

# ── Directorio cache de Ivy (evita re-descargas de paquetes Maven) ─────────────
mkdir -p ivy2-cache

# ── Placeholder de kubeconfig para Airflow ────────────────────────────────────
# Docker crea un DIRECTORIO si el origen de un bind-mount no existe.
# Con este placeholder se monta como fichero. El contenido real se copia
# desde ~/.kube/config en el Paso 6 (start_k8s.sh).
if [ ! -f "airflow/k8s-spark-config.yaml" ]; then
  # Si existe como directorio (ejecuciones previas sin el placeholder), lo borramos
  [ -d "airflow/k8s-spark-config.yaml" ] && rm -rf "airflow/k8s-spark-config.yaml"
  echo "# Kubeconfig placeholder — rellenado automáticamente por start_k8s.sh" \
    > "airflow/k8s-spark-config.yaml"
  echo "[extra] Placeholder de kubeconfig de Airflow creado."
fi

echo ""
echo "========================================================"
echo " Preparación completada."
echo " Ahora puedes ejecutar: docker compose up --build -d"
echo "========================================================"
