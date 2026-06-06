# Práctica Creativa — Predicción de Retraso de Vuelos en Tiempo Real

Sistema de predicción en tiempo real que combina **Kafka**, **Spark Streaming**, **Cassandra** y **Flask**, desplegado en **Google Kubernetes Engine (GKE)**.

El usuario envía una solicitud de predicción desde la web; Spark la procesa con un modelo RandomForest entrenado con MLlib y devuelve el resultado al navegador vía WebSocket en ~5-10 segundos.

## Arquitectura

```
                  ┌──────────┐   Kafka request    ┌──────────────────────┐
  Navegador ────▶ │  Flask   │ ─────────────────▶ │   Spark Streaming    │
                  │ :5001    │                     │ (FlightDelayPredictor│
                  │          │ ◀───────────────── │  en cluster Spark)   │
                  └──────────┘  WebSocket result  └──────────────────────┘
                                                           │
                                        ┌──────────────────┼──────────────────┐
                                        ▼                  ▼                  ▼
                                    Cassandra            MinIO             MLflow
                                  (resultados)         (modelos)        (tracking)
```

**Servicios desplegados en K8s:**

| Servicio       | Descripción                                    |
|----------------|------------------------------------------------|
| Flask          | Aplicación web con UI de predicción (puerto 80)|
| Spark Master   | Coordinador del cluster Spark                  |
| Spark Workers  | 2 réplicas que ejecutan las predicciones       |
| Kafka          | Bus de mensajes (KRaft, sin Zookeeper)         |
| Cassandra      | Base de datos de resultados y distancias       |
| MinIO          | Almacén S3-compatible para modelos ML          |
| MLflow         | Tracking de experimentos de entrenamiento      |

---

## Prerrequisitos

- **Cuenta de Google Cloud** con billing habilitado
- **`gcloud` CLI** instalado y autenticado en tu máquina local:
  ```bash
  gcloud auth login
  gcloud config set project TU_PROJECT_ID
  ```
- **Docker Engine** en la VM de GCloud (instalación en el Paso 2)

---

## Despliegue en Kubernetes (GKE)

### Paso 1 — Crear la VM de trabajo y abrir puertos

Desde tu máquina local o Cloud Shell:

```bash
# Crear la VM (sirve como nodo de build y para alojar los modelos)
gcloud compute instances create practica-vm \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=60GB \
  --zone=europe-west1-b \
  --tags=practica-ports \
  --scopes=cloud-platform

# Abrir puertos de administración
gcloud compute firewall-rules create practica-allow-ports \
  --allow tcp:5001,tcp:8080,tcp:8090,tcp:9000,tcp:9001,tcp:5000 \
  --target-tags=practica-ports \
  --description="Puertos de practica-creativa"
```

### Paso 2 — Conectarse a la VM e instalar Docker y kubectl

```bash
# Conectarse
gcloud compute ssh practica-vm --zone=europe-west1-b

# Instalar Docker (dentro de la VM)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker --version   # debe imprimir la versión instalada

# Instalar kubectl y gke-gcloud-auth-plugin
# (la imagen Ubuntu 22.04 de GCE tiene gcloud vía apt sin estos paquetes)
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
  | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update -qq && \
sudo apt-get install -y kubectl google-cloud-sdk-gke-gcloud-auth-plugin
echo 'export USE_GKE_GCLOUD_AUTH_PLUGIN=True' >> ~/.bashrc && source ~/.bashrc
kubectl version --client   # debe imprimir la versión instalada
```

### Paso 3 — Clonar el repositorio y preparar el entorno

```bash
git clone https://github.com/Mauro1455/practica_creativa.git
cd practica_creativa

# Descarga Spark 4.1.1, JARs S3A, datos de distancias y el JAR precompilado (~550 MB, ~5-10 min)
bash prepare.sh
```

El script `prepare.sh` realiza 6 pasos:
1. Spark 4.1.1 binarios
2. `extra-jars/hadoop-aws-3.4.1.jar` (acceso S3A → MinIO; en `extra-jars/` porque Spark 4.1.1 incluye `hadoop-client-runtime-3.4.2` con formato "60s" que rompe 3.3.x)
3. `extra-jars/bundle-2.25.16.jar` (AWS SDK v2 — requerido por hadoop-aws 3.4.x; en `extra-jars/` para que el Spark Master **no** lo cargue automáticamente y evitar el conflicto Netty con `NoSuchMethodError` en `PooledByteBufAllocator`)
4. `data/origin_dest_distances.jsonl` (distancias entre aeropuertos, necesario para Cassandra)
5. JAR precompilado de Scala (evita instalar sbt)
6. `jars/java-driver-core-shaded-4.18.1.jar` (DataStax OSS Driver v4 para Cassandra; marcado como `provided` en build.sbt → debe estar en el classpath del worker en runtime; shaded = sin conflictos Netty)

### Paso 4 — Arrancar los servicios de soporte con Docker Compose

Docker Compose levanta MinIO, MLflow, Kafka y Cassandra. El entrenamiento ocurre en el Paso 6 sobre el cluster GKE (cluster mode real).

```bash
# Arrancar los servicios de soporte
docker compose up --build -d

# Verificar que todos los contenedores estén en marcha (~5 min la primera vez)
docker compose ps
```

> **Importante:** mantén Docker Compose en marcha durante el Paso 6. Los pods de entrenamiento en GKE acceden al MinIO y MLflow de esta VM por su IP pública.

### Paso 5 — Crear el cluster GKE y el Artifact Registry

```bash
# Crear Artifact Registry (donde se subirán las imágenes Docker)
gcloud artifacts repositories create practica \
  --repository-format=docker \
  --location=europe-west1 \
  --project=$(gcloud config get-value project)

# Crear el cluster GKE con 2 nodos
gcloud container clusters create practica-k8s \
  --num-nodes=2 \
  --machine-type=e2-standard-4 \
  --disk-type=pd-standard \
  --disk-size=50 \
  --zone=europe-west1-b \
  --scopes=cloud-platform \
  --project=$(gcloud config get-value project)

# Dar permiso de lectura al service account del nodo para Artifact Registry
# (necesario para que los pods puedan hacer pull de las imágenes)
PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')
gcloud projects add-iam-policy-binding $(gcloud config get-value project) \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

El cluster tarda ~5 minutos en estar disponible.

### Paso 6 — Construir imágenes, entrenar en K8s y desplegar

Con Docker Compose en marcha, ejecutar estos tres comandos en orden:

```bash
# 6a — Build + push imágenes + desplegar servicios K8s (~20 min)
#      Construye spark-base (incluye Python para el training), spark-predictor y flask.
#      Despliega Kafka, Cassandra, MinIO, Spark, Flask en GKE.
bash k8s/start_k8s.sh --skip-models

# 6b — Entrenar el modelo RandomForest en K8s en cluster mode (~10-15 min)
#      El driver y los executors corren como pods de GKE.
#      El modelo se guarda en el MinIO de Docker Compose (esta VM).
#      Progreso: http://$(curl -s ifconfig.me):5000 → Experiments
bash train.sh

# 6c — Copiar modelos al MinIO de K8s y arrancar el predictor (~5 min)
bash k8s/start_k8s.sh --skip-build --skip-deploy
```

**Por qué tres comandos:**
- `--skip-models` en el primero: no hay modelos todavía, se entrena después
- `train.sh` usa `--master k8s://... --deploy-mode cluster` — funciona con Python porque K8s sí soporta Python en cluster mode (a diferencia de Spark Standalone)
- `--skip-build --skip-deploy` en el último: solo copia modelos y relanza el predictor

> **Arquitectura del predictor en K8s:** el Job `spark-predictor` actúa como cliente de spark-submit; tras enviar el driver al cluster Spark Standalone de K8s, el pod del Job termina (`Completed`) y el driver sigue corriendo en uno de los pods `spark-worker`.

### Paso 7 — Obtener la URL de la aplicación

```bash
# Obtener la IP externa del servicio Flask (puede tardar 2-3 min en asignarse)
kubectl get svc flask -n practica
```

La aplicación está disponible en:
```
http://<EXTERNAL-IP>/flights/delays/predict_kafka
```

---

## Verificación del funcionamiento

### Predicción en tiempo real (F12 → Network → WS)

1. Abre `http://<EXTERNAL-IP>/flights/delays/predict_kafka`
2. Abre **DevTools** (`F12`) → pestaña **Network** → filtra por **WS**
3. Rellena el formulario:
   - **Departure Delay**: `15`
   - **Carrier**: `AA`
   - **Date**: `2016-12-25`
   - **Origin**: `ATL`
   - **Destination**: `SFO`
4. Pulsa **Submit**
5. En DevTools verás el frame WebSocket con la predicción llegando en ~5-10 segundos

### Spark UI

```bash
kubectl port-forward -n practica svc/spark-master-svc 8080:8080
```

Abre `http://localhost:8080` — debe aparecer el job `FlightDelayPredictor` en estado **RUNNING**.

### MinIO (modelos)

```bash
kubectl port-forward -n practica svc/minio 9001:9001
```

Abre `http://localhost:9001` (usuario: `minio`, contraseña: `minio123`) → bucket `lakehouse` → carpeta `models/` → debe contener `spark_random_forest_classifier.flight_delays.5.0.bin/`.

### Comandos útiles de diagnóstico

```bash
# Estado de todos los pods
kubectl get pods -n practica

# Logs del Job spark-predictor (muestra la sumisión del driver)
kubectl logs -n practica job/spark-predictor

# Logs del driver Spark en tiempo real (corre en un worker pod)
kubectl exec -n practica -l app=spark-worker -- \
  bash -c "tail -f /var/lib/spark/work/driver-*/stdout 2>/dev/null"

# Confirmar que el driver está en cluster mode
kubectl logs -n practica job/spark-predictor | grep "Driver running on"

# Logs del Flask en tiempo real
kubectl logs -n practica -l app=flask -f
```

---

## Requisitos completados

| # | Requisito                                                        | Estado |
|---|------------------------------------------------------------------|--------|
| 1 | Predicción en tiempo real vía Kafka + Spark Streaming           | ✅     |
| 2 | Modelo RandomForest entrenado con Spark MLlib                    | ✅     |
| 3 | Almacenamiento de resultados en Cassandra                        | ✅     |
| 4 | Interfaz web con WebSockets (Flask + SocketIO)                   | ✅     |
| 5 | Pipeline de entrenamiento con Spark MLlib (`bash train.sh`)      | ✅     |
| 6 | Almacén de artefactos con MinIO (S3-compatible)                  | ✅     |
| 7 | Tracking de experimentos con MLflow                              | ✅     |
| 8 | Despliegue en Kubernetes (GKE) con manifiestos declarativos      | ✅     |
| 9 | Imágenes Docker en Artifact Registry                             | ✅     |
|10 | Script de despliegue automatizado (`k8s/start_k8s.sh`)           | ✅     |
