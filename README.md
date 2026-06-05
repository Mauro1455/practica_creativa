# Práctica Creativa — Predicción de Retraso de Vuelos en Tiempo Real

Sistema de predicción en tiempo real que combina **Kafka**, **Spark Streaming**, **Cassandra** y **Flask**. El usuario envía una solicitud de predicción desde la web; Spark la procesa con un modelo RandomForest entrenado con MLlib y devuelve el resultado vía WebSocket.

## Arquitectura

```
                  ┌──────────┐   Kafka request    ┌──────────────────┐
  Navegador ───▶  │  Flask   │ ──────────────────▶ │  Spark Streaming │
                  │  :5001   │                     │  (FlightPredictor│
                  │          │ ◀────────────────── │   sobre cluster) │
                  └──────────┘   Cassandra result  └──────────────────┘
                  WebSocket ▲                               │
                            │                              ▼
                        Browser                       Cassandra
                                                    (origin_dest_distances
                                                     + flight_delay_ml_response)
```

**Servicios incluidos:**

| Servicio        | Puerto | Descripción                              |
|-----------------|--------|------------------------------------------|
| Flask           | 5001   | Aplicación web (UI de predicción)        |
| Spark Master    | 8080   | Web UI del cluster Spark                 |
| Spark Worker 1  | 8083   | Worker UI                                |
| Spark Worker 2  | 8082   | Worker UI                                |
| Kafka           | 9092   | Bus de mensajes (KRaft, sin Zookeeper)   |
| Cassandra       | 9042   | Base de datos de resultados              |
| MinIO           | 9000/9001 | Almacén de modelos ML (S3-compatible) |
| MLflow          | 5000   | Tracking de experimentos                 |
| Airflow         | 8090   | Scheduler para re-entrenamiento          |

---

## Requisitos previos (comunes a ambas opciones)

- **Cuenta de Google Cloud** con billing habilitado
- **`gcloud` CLI** instalado y autenticado:
  ```bash
  gcloud auth login
  gcloud config set project TU_PROJECT_ID
  ```
- **VM de GCloud** con al menos:
  - Tipo: `e2-standard-4` (4 vCPU, 16 GB RAM)
  - SO: Ubuntu 22.04 LTS
  - Disco: 60 GB
  - **Docker Engine** instalado (ver Paso 1B)

---

## OPCIÓN A — Docker Compose en VM de GCloud

### Paso 1A — Crear la VM y abrir puertos de firewall

Ejecuta desde tu máquina local (o desde Cloud Shell):

```bash
# Crear la VM
gcloud compute instances create practica-vm \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=60GB \
  --zone=europe-west1-b \
  --tags=practica-ports

# Abrir puertos de la aplicación
gcloud compute firewall-rules create practica-allow-ports \
  --allow tcp:5001,tcp:8080,tcp:8082,tcp:8083,tcp:8090,tcp:9001,tcp:5000 \
  --target-tags=practica-ports \
  --description="Puertos de practica-creativa"
```

### Paso 1B — Instalar Docker en la VM

```bash
# Conectarse a la VM
gcloud compute ssh practica-vm --zone=europe-west1-b

# Instalar Docker (dentro de la VM)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# Verificar instalación
docker --version
```

### Paso 2 — Clonar el repositorio y preparar el entorno

```bash
# Clonar el repositorio
git clone https://github.com/mauper922/practica_creativa.git
cd practica_creativa

# Descargar Spark 4.1.1 y los JARs necesarios (~830 MB en total)
# Este paso tarda ~5-10 minutos según la velocidad de la conexión
bash prepare.sh
```

El script `prepare.sh`:
- Descarga **Spark 4.1.1** a `./spark-4.1.1/`
- Descarga los JARs de **hadoop-aws** y **aws-java-sdk-bundle** (necesarios para conectar con MinIO)
- Copia el **JAR precompilado** de Scala al directorio esperado (evita instalar sbt)
- Crea el directorio `./ivy2-cache/` para cachear paquetes Maven

> **Nota:** `spark-4.1.1/` e `ivy2-cache/` están excluidos del repositorio (`.gitignore`). Solo se crean localmente y no se suben a git.

### Paso 3 — Arrancar todos los servicios

```bash
# Primera vez: construye imágenes y arranca todos los servicios en segundo plano
docker compose up --build -d
```

La primera ejecución tarda **10-15 minutos** (descarga de imágenes Docker). Las siguientes son más rápidas.

Para ver el progreso en tiempo real:

```bash
docker compose logs -f
```

Para ver el estado de los contenedores:

```bash
docker compose ps
```

Todos los servicios deben aparecer como `running` (algunos como `exited 0` si son jobs de init, lo cual es correcto).

### Paso 4 — Obtener la IP externa de la VM

```bash
# Desde tu máquina local (no desde la VM):
gcloud compute instances describe practica-vm \
  --zone=europe-west1-b \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

### Paso 5 — Entrenar el modelo ML con Airflow

> El predictor Spark necesita un modelo entrenado en MinIO. Sin este paso, la UI carga pero las predicciones no llegan.

1. Abre el **Airflow UI** en: `http://<VM_IP>:8090`
   - Usuario: `admin`
   - Contraseña: `admin`

2. Ve a **DAGs** → `train_flight_delay_model`

3. Haz clic en el botón **▶ Trigger DAG** (esquina superior derecha)

4. Espera **~10 minutos** hasta que el estado del DAG cambie a verde (✓ success)

5. Una vez completado el entrenamiento, **reinicia el predictor** para que cargue el modelo:
   ```bash
   docker compose restart spark-predictor
   ```
   Espera ~30 segundos a que el predictor establezca conexión con Kafka.

### Paso 6 — Probar la aplicación

Abre en el navegador:

```
http://<VM_IP>:5001/flights/delays/predict_kafka
```

Rellena el formulario y pulsa **Submit**. La predicción aparece en pantalla en ~5-10 segundos.

---

## OPCIÓN B — Kubernetes en GKE

> **Prerrequisito:** Tener la **Opción A funcionando** con el modelo entrenado en MinIO. El script de k8s copia el modelo desde el MinIO de Docker Compose al MinIO de K8s.

### Paso 1 — Instalar kubectl

```bash
# Dentro de la VM (donde corre Docker Compose):
snap install kubectl --classic
```

### Paso 2 — Crear el cluster GKE

```bash
# Crear cluster (3 nodos n2-standard-4)
gcloud container clusters create practica-k8s \
  --num-nodes=3 \
  --machine-type=n2-standard-4 \
  --zone=europe-west1-b \
  --project=$(gcloud config get-value project)
```

Tarda ~5 minutos.

### Paso 3 — Crear el Artifact Registry

```bash
gcloud artifacts repositories create practica \
  --repository-format=docker \
  --location=europe-west1 \
  --project=$(gcloud config get-value project)
```

### Paso 4 — Desplegar en Kubernetes

Asegúrate de que Docker Compose sigue corriendo (necesita el MinIO local con los modelos):

```bash
docker compose ps   # MinIO debe estar "running"
```

Ejecuta el script de despliegue desde el directorio del proyecto:

```bash
bash k8s/start_k8s.sh
```

Este script (~20-30 minutos en total):
1. Autentica Docker con Artifact Registry
2. Construye y sube las imágenes (`spark-base`, `spark-predictor`, `flask`)
3. Configura `kubectl` para el cluster GKE
4. Aplica todos los manifiestos de Kubernetes
5. Copia los modelos ML del MinIO local al MinIO de K8s
6. Espera a que el predictor Spark esté activo

### Paso 5 — Acceder a la aplicación

```bash
# Obtener la IP externa del servicio Flask (puede tardar 2-3 minutos)
kubectl get svc flask -n practica
```

La columna `EXTERNAL-IP` contiene la IP. Accede a:

```
http://<EXTERNAL_IP>/flights/delays/predict_kafka
```

#### Comandos útiles para K8s

```bash
# Estado de todos los pods
kubectl get pods -n practica

# Logs del predictor Spark
kubectl logs -n practica job/spark-predictor

# Logs del Flask
kubectl logs -n practica -l app=flask -f

# Spark Web UI via port-forward
kubectl port-forward -n practica svc/spark-master-svc 8080:8080
# Luego abrir: http://localhost:8080

# MinIO Console via port-forward
kubectl port-forward -n practica svc/minio 9001:9001
# Luego abrir: http://localhost:9001  (minio / minio123)
```

---

## Verificación del funcionamiento

### 1. Comprobar que los servicios están en marcha

```bash
# Docker Compose
docker compose ps

# Kubernetes
kubectl get pods -n practica
```

### 2. Verificar la predicción en tiempo real (F12 → Network → WS)

1. Abre `http://<IP>:5001/flights/delays/predict_kafka`
2. Abre las **DevTools** del navegador (`F12`) → pestaña **Network** → filtra por **WS**
3. Rellena el formulario con estos valores de prueba:
   - **Departure Delay**: `15`
   - **Carrier**: `AA`
   - **Date**: `2016-12-25`
   - **Origin**: `ATL`
   - **Destination**: `SFO`
4. Pulsa **Submit**
5. En DevTools verás el frame WebSocket con la predicción llegando en ~5-10 segundos
6. El resultado se muestra en la página

### 3. Verificar el Spark UI

Abre `http://<IP>:8080` y comprueba que aparece el job `FlightDelayPredictor` en estado `RUNNING` (sección *Running Applications*).

### 4. Verificar los modelos en MinIO

Abre `http://<IP>:9001` (usuario: `minio`, contraseña: `minio123`) → bucket `lakehouse` → carpeta `models/` → debe contener la carpeta `spark_random_forest_classifier.flight_delays.5.0.bin/`.

### 5. Verificar Cassandra (opcional)

```bash
# Docker Compose
docker compose exec cassandra cqlsh -e \
  "SELECT * FROM agile_data_science.flight_delay_ml_response LIMIT 5;"
```

---

## Resolución de problemas

### `spark-predictor` falla al cargar el modelo

**Síntoma:** `docker compose logs spark-predictor` muestra error de S3 o modelo no encontrado.

**Causa:** el DAG de Airflow aún no ha terminado de entrenar el modelo.

```bash
# Ver estado del entrenamiento
docker compose logs airflow-scheduler --tail=50

# Una vez que el DAG completa, reiniciar el predictor
docker compose restart spark-predictor
```

### El DAG de Airflow falla (estado rojo)

```bash
docker compose logs airflow-scheduler --tail=100
```

Si el error es `spark-submit: command not found`:

```bash
# El directorio spark-4.1.1/ no existe — ejecutar prepare.sh de nuevo
bash prepare.sh
docker compose restart airflow-scheduler
```

### La predicción nunca llega a la UI (WebSocket conectado pero sin respuesta)

```bash
# Verificar que el predictor está consumiendo de Kafka
docker compose logs spark-predictor --tail=20

# Verificar que el driver de Spark está corriendo en los workers
docker compose logs spark-worker-1 --tail=30
docker compose logs spark-worker-2 --tail=30

# Ver los logs del Flask
docker compose logs flask --tail=20
```

### Error de memoria en algún servicio

Verificar que la VM tiene al menos 16 GB de RAM:

```bash
free -h
```

Si la memoria es insuficiente, aumentar el tipo de máquina:

```bash
# Desde tu máquina local:
gcloud compute instances stop practica-vm --zone=europe-west1-b
gcloud compute instances set-machine-type practica-vm \
  --machine-type=e2-highmem-4 \
  --zone=europe-west1-b
gcloud compute instances start practica-vm --zone=europe-west1-b
```

### Reinicio completo desde cero

```bash
# Parar y eliminar todos los contenedores y volúmenes
docker compose down -v

# Limpiar la caché de MinIO (modelos)
rm -rf data_minio/

# Volver a arrancar
docker compose up --build -d
# (Recordar volver a entrenar el modelo con Airflow después)
```

---

## Estructura del proyecto

```
practica_creativa/
├── docker-compose.yml          # Orquestación de todos los servicios
├── prepare.sh                  # Script de preparación (descarga Spark)
├── cassandra-init.sh           # Inicialización de Cassandra
├── airflow/
│   └── dags/
│       └── train_model_dag.py  # DAG de entrenamiento del modelo
├── flight_prediction/
│   ├── Dockerfile.spark        # Imagen del predictor Spark
│   ├── entrypoint.sh           # Lanza spark-submit en cluster mode
│   └── src/                    # Código Scala (MakePrediction)
├── resources/
│   ├── train_spark_mllib_model.py  # Entrenamiento del modelo MLlib
│   └── web/
│       ├── Dockerfile.flask    # Imagen de la aplicación Flask
│       ├── predict_flask.py    # Servidor Flask + WebSocket
│       └── predict_utils.py    # Utilidades (Cassandra, fechas)
├── k8s/
│   ├── start_k8s.sh            # Script completo de despliegue en GKE
│   ├── setup.sh                # Construye y sube imágenes
│   ├── apply.sh                # Aplica manifiestos kubectl
│   ├── Dockerfile.spark-base   # Imagen base de Spark para K8s
│   └── *.yaml                  # Manifiestos de Kubernetes
└── data/
    └── origin_dest_distances.jsonl  # Distancias entre aeropuertos
```

---

## Flujo de datos completo

```
1. Usuario rellena formulario → POST /flights/delays/predict/classify_realtime
2. Flask publica mensaje JSON en topic Kafka "flight-delay-ml-request"
3. Flask emite UUID al cliente vía WebSocket (room=UUID)
4. Spark Streaming consume el mensaje de Kafka
5. Spark carga modelo RandomForest desde MinIO (s3a://lakehouse/models/)
6. Spark hace la predicción y escribe el resultado en Cassandra
7. Spark publica el resultado en topic Kafka "flight-delay-ml-response"
8. Flask (consumer thread) lee de Kafka y emite vía WebSocket al room=UUID
9. El navegador recibe la predicción y la muestra en pantalla
```
