# Despliegue en Kubernetes — Practica Creativa

Cluster GKE: 2 nodos `e2-standard-4` (4 CPU / 16 GB RAM c/u) en `europe-west1-b`.

## Arquitectura

```
                          ┌─────────────────────────────────────┐
                          │           Namespace: practica        │
                          │                                      │
  Internet ──── LB:80 ──► │  flask:5001                          │
                          │     ↕                                │
                          │  kafka:9092 ◄─── spark-predictor ───►│
                          │     ↕               (Job, 1-shot)    │
                          │  cassandra:9042                      │
                          │     ↕                                │
                          │  spark-master:7077/6066              │
                          │     ↕                                │
                          │  spark-worker (×2)                   │
                          │     ↕                                │
                          │  minio:9000  ◄── models, jars        │
                          │  jar-server:8888 ◄── JAR             │
                          └─────────────────────────────────────┘
```

## Pre-requisitos

```bash
# Verificar contexto kubectl apunta al cluster correcto
kubectl config current-context

# Verificar que el JAR está compilado
ls flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar

# Si no está compilado:
cd flight_prediction && sbt package && cd ..
```

---

## Despliegue automático (recomendado)

Ejecuta el script desde la raíz del proyecto. Hace todo:

```bash
cd /home/mauper922/practica_creativa
bash k8s/setup.sh
```

El script:
1. Crea el repositorio en Artifact Registry
2. Construye y sube las 3 imágenes Docker (`spark-base`, `spark-predictor`, `flask`)
3. Aplica los manifiestos en orden
4. Crea los ConfigMaps con datos (distancias, JAR)
5. Hace seed de MinIO con los modelos Spark y sklearn
6. Espera a que los jobs de init completen
7. Envía el job del predictor Spark
8. Muestra la IP pública de Flask

---

## Despliegue manual (paso a paso)

### Paso 0 — Variables de entorno

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=europe-west1
export REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/practica"
```

### Paso 1 — Artifact Registry

```bash
gcloud artifacts repositories create practica \
  --repository-format=docker \
  --location=$REGION
gcloud auth configure-docker ${REGION}-docker.pkg.dev
```

### Paso 2 — Construir imágenes

```bash
# 2a. spark-base (incluye Spark 4.1.1 + S3A jars)
BUILD_CTX=$(mktemp -d)
rsync -a --exclude='work/' /home/mauper922/spark-4.1.1/ $BUILD_CTX/spark-4.1.1/
cp k8s/Dockerfile.spark-base $BUILD_CTX/Dockerfile
docker build -t ${REGISTRY}/spark-base:4.1.1 $BUILD_CTX
docker push ${REGISTRY}/spark-base:4.1.1

# 2b. spark-predictor (FROM spark-base + JAR + entrypoint)
PRED_CTX=$(mktemp -d)
cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar $PRED_CTX/
cp flight_prediction/entrypoint.sh $PRED_CTX/
cp k8s/Dockerfile.spark-predictor $PRED_CTX/Dockerfile
docker build --build-arg BASE_IMAGE=${REGISTRY}/spark-base:4.1.1 \
  -t ${REGISTRY}/spark-predictor:latest $PRED_CTX
docker push ${REGISTRY}/spark-predictor:latest

# 2c. flask
docker build -t ${REGISTRY}/flask:latest \
  -f resources/web/Dockerfile.flask resources/web/
docker push ${REGISTRY}/flask:latest
```

### Paso 3 — Namespace

```bash
kubectl apply -f k8s/00-namespace.yaml
```

### Paso 4 — ConfigMaps de datos

```bash
# Datos de distancias para Cassandra
kubectl create configmap cassandra-init-data \
  --from-file=origin_dest_distances.jsonl=data/origin_dest_distances.jsonl \
  -n practica

# JAR del predictor para el jar-server HTTP
kubectl create configmap spark-jar \
  --from-file=flight_prediction_2.13-0.1.jar=\
    flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar \
  -n practica
```

### Paso 5 — Infraestructura base

```bash
REGISTRY=$REGISTRY envsubst '${REGISTRY}' < k8s/01-kafka.yaml | kubectl apply -f -
kubectl apply -f k8s/02-kafka-init-job.yaml

REGISTRY=$REGISTRY envsubst '${REGISTRY}' < k8s/03-cassandra.yaml | kubectl apply -f -
kubectl apply -f k8s/04-cassandra-init-job.yaml

kubectl apply -f k8s/05-minio.yaml
```

> **Esperar** a que Cassandra esté Ready (puede tardar 2-3 min):
> ```bash
> kubectl get pods -n practica -w
> ```

### Paso 6 — Seed de MinIO

```bash
# Port-forward MinIO temporalmente
kubectl port-forward -n practica svc/minio 9000:9000 &
sleep 5

mc alias set k8sminio http://localhost:9000 minio minio123
mc mb --ignore-existing k8sminio/lakehouse

# Subir modelos Spark ML y sklearn
mc cp --recursive models/ k8sminio/lakehouse/models/

# Subir el JAR (opcional, algunos Spark jobs lo leen desde MinIO)
mc cp flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar \
   k8sminio/lakehouse/jars/

kill %1  # detener port-forward
```

### Paso 7 — Spark y servicios de aplicación

```bash
REGISTRY=$REGISTRY envsubst '${REGISTRY}' < k8s/06-spark.yaml | kubectl apply -f -
kubectl apply -f k8s/07-jar-server.yaml
REGISTRY=$REGISTRY envsubst '${REGISTRY}' < k8s/09-flask.yaml | kubectl apply -f -
```

### Paso 8 — Verificar init jobs

```bash
# Esperar a que completen
kubectl wait --for=condition=complete job/cassandra-init -n practica --timeout=600s
kubectl wait --for=condition=complete job/kafka-init -n practica --timeout=120s
```

### Paso 9 — Enviar el job del predictor Spark

```bash
# Esperar a que Spark esté listo
kubectl rollout status deployment/spark-master -n practica --timeout=120s
kubectl rollout status deployment/spark-worker -n practica --timeout=180s

REGISTRY=$REGISTRY envsubst '${REGISTRY}' < k8s/08-spark-predictor-job.yaml | kubectl apply -f -
```

### Paso 10 — IP pública de Flask

```bash
kubectl get svc flask -n practica
# Copiar EXTERNAL-IP y abrir en el navegador (puerto 80)
```

---

## Verificación

```bash
# Estado general
kubectl get all -n practica

# Logs del predictor Spark
kubectl logs -n practica job/spark-predictor

# Logs de Flask
kubectl logs -n practica -l app=flask

# Spark Web UI
kubectl port-forward -n practica svc/spark-master 8080:8080
# → http://localhost:8080

# MinIO Console
kubectl port-forward -n practica svc/minio 9001:9001
# → http://localhost:9001  (minio / minio123)
```

---

## Orden de dependencias

```
00-namespace
   ↓
01-kafka ──────────────────────────────────────────┐
02-kafka-init-job (Job, espera kafka ready)         │
   ↓                                               │
03-cassandra                                       │
04-cassandra-init-job (Job, espera cassandra CQL)  │
   ↓                                               │
05-minio + seed manual con mc                      │
   ↓                                               │
06-spark (master + 2 workers)                      │
07-jar-server                                      │
09-flask ◄─────────────────────────────────────────┘
   ↓ (cuando init jobs completan)
08-spark-predictor-job (Job, 1-shot)
```

---

## Recursos consumidos (~12 GB RAM, ~5 CPUs de los 28 GB / 6 CPUs disponibles)

| Componente       | CPU req | RAM req |
|-----------------|---------|---------|
| kafka            | 250m    | 512Mi   |
| cassandra        | 500m    | 1Gi     |
| minio            | 100m    | 256Mi   |
| spark-master     | 250m    | 512Mi   |
| spark-worker ×2  | 1000m   | 3Gi     |
| jar-server       | 50m     | 64Mi    |
| flask            | 100m    | 256Mi   |
| **Total**        | **~2.3**| **~5.6Gi** |

---

## Solución de problemas

### Kafka no arranca (KRaft format error)
```bash
# Eliminar el PVC y el StatefulSet para reformatear
kubectl delete statefulset kafka -n practica
kubectl delete pvc kafka-data-kafka-0 -n practica
kubectl apply -f k8s/01-kafka.yaml
```

### Cassandra init falla por timeout
```bash
kubectl logs -n practica job/cassandra-init
# Si Cassandra tardó más: borrar y relanzar el job
kubectl delete job cassandra-init -n practica
kubectl apply -f k8s/04-cassandra-init-job.yaml
```

### spark-predictor no puede descargar paquetes Maven
```bash
# Verificar que los workers tienen acceso a internet
kubectl exec -n practica -l app=spark-worker -- curl -s https://repo1.maven.org/maven2/ | head -5
```

### Relanzar el job del predictor (si el driver Spark fue desalojado)
```bash
kubectl delete job spark-predictor -n practica
kubectl apply -f k8s/08-spark-predictor-job.yaml
```
