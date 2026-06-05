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
  --tags=practica-ports

# Abrir puertos de administración
gcloud compute firewall-rules create practica-allow-ports \
  --allow tcp:5001,tcp:8080,tcp:8090,tcp:9000,tcp:9001,tcp:5000 \
  --target-tags=practica-ports \
  --description="Puertos de practica-creativa"
```

### Paso 2 — Conectarse a la VM e instalar Docker

```bash
# Conectarse
gcloud compute ssh practica-vm --zone=europe-west1-b

# Instalar Docker (dentro de la VM)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker --version   # debe imprimir la versión instalada
```

### Paso 3 — Clonar el repositorio y preparar el entorno

```bash
git clone https://github.com/Mauro1455/practica_creativa.git
cd practica_creativa

# Descarga Spark 4.1.1, JARs S3A y copia el JAR precompilado (~830 MB, ~5-10 min)
bash prepare.sh
```

El script `prepare.sh` descarga y coloca en `./spark-4.1.1/jars/`:
- Spark 4.1.1 binarios
- `hadoop-aws-3.4.1.jar` y `bundle-2.25.16.jar` (AWS SDK v2, necesario para conectar MinIO con hadoop-aws 3.4.1)
- JAR precompilado de Scala (evita instalar sbt)

### Paso 4 — Entrenar el modelo ML con Docker Compose

El cluster K8s necesita un modelo RandomForest entrenado. El script `start_k8s.sh` lo copia automáticamente del MinIO local al MinIO de K8s, así que hay que entrenar antes de desplegar:

```bash
# Arrancar los servicios de soporte (Spark, MinIO, Kafka, Cassandra, MLflow)
docker compose up --build -d

# Esperar a que todos los contenedores estén en marcha (~5 min la primera vez)
docker compose ps

# Entrenar el modelo con Spark MLlib en modo cluster (~10-15 min)
bash train.sh
```

El script `train.sh` lanza un `spark-submit --deploy-mode cluster` que entrena un RandomForest con Spark MLlib y guarda el modelo en MinIO (`lakehouse/models/`).

El progreso puede monitorizarse en **MLflow**: `http://$(curl -s ifconfig.me):5000` → sección **Experiments** → aparecerá un run activo con métricas en tiempo real.

> **Importante:** no pares Docker Compose después del entrenamiento. El paso 6 lo necesita levantado para copiar los modelos al MinIO de K8s.

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
  --project=$(gcloud config get-value project)
```

El cluster tarda ~5 minutos en estar disponible. `kubectl` ya viene preinstalado en Cloud Shell y en las VMs de GCloud; no es necesario instalarlo.

### Paso 6 — Desplegar en Kubernetes

Con Docker Compose aún en marcha (necesario para copiar los modelos al MinIO de K8s):

```bash
bash k8s/start_k8s.sh
```

El script realiza automáticamente en ~20-30 minutos:

1. **Autenticación** de Docker con Artifact Registry
2. **Build y push** de imágenes (`spark-base`, `spark-predictor`, `flask`)
3. **Configuración de kubectl** para el cluster GKE
4. **Despliegue** de todos los manifiestos (Kafka, Cassandra, MinIO, Spark Master/Workers, Flask)
5. **Copia de modelos** del MinIO local (Docker Compose) al MinIO de K8s
6. **Lanzamiento del predictor** Spark en modo cluster (`--deploy-mode cluster`): el driver corre en un worker pod, no en el pod del job
7. **Verificación** de que el predictor está consumiendo mensajes de Kafka

Al finalizar, el script imprime la URL de acceso.

> **Arquitectura del predictor en K8s:** el Job `spark-predictor` actúa como cliente de spark-submit; tras enviar el driver al cluster Spark, el pod del Job termina (`Completed`) y el driver sigue corriendo en uno de los pods `spark-worker`.

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
