# Contexto del proyecto — Práctica Creativa ETSIT (Big Data)

## Qué hace esta práctica

Sistema de predicción de retrasos de vuelos en tiempo real con arquitectura distribuida completa:

1. El usuario rellena un formulario web (Flask en puerto 5001)
2. Flask publica la petición en Kafka (`flight-delay-ml-request`)
3. Spark Streaming consume Kafka, aplica un modelo Random Forest, y escribe el resultado en:
   - Kafka (`flight-delay-ml-response`)
   - Cassandra (`agile_data_science.flight_delay_ml_response`)
4. Flask recibe la predicción via WebSocket (Socket.IO) y la muestra al usuario

---

## Arquitectura de servicios (docker-compose)

```
kafka ──► kafka-init (crea topics)
cassandra ──► cassandra-init (crea keyspace, tablas, importa distancias)
minio (almacena modelos ML y checkpoints de Spark)
jar-server (sirve el JAR de Scala por HTTP al cluster Spark)
spark-master (puerto 7077 RPC, 8080 UI, 6066 REST cluster mode)
  ├── spark-worker-1 (puerto 8083 UI)
  └── spark-worker-2 (puerto 8082 UI)
spark-predictor (solo hace spark-submit y sale con código 0 — normal)
flask (puerto 5001)
```

### URLs útiles
- Aplicación web: http://localhost:5001/flights/delays/predict_kafka
- Spark Master UI: http://localhost:8080 (debe mostrar Workers: 2, Running Drivers: 1)
- Spark Worker 1 UI: http://localhost:8083
- Spark Worker 2 UI: http://localhost:8082
- MinIO console: http://localhost:9001 (minio/minio123)

---

## Modos de ejecución de Spark

### CRÍTICO — deploy-mode cluster
La práctica usa `--deploy-mode cluster`. Esto significa:
- El contenedor `spark-predictor` solo hace el submit y **sale con código 0** (eso es correcto, no es un error)
- El driver real corre dentro de uno de los workers
- Las variables de entorno (`KAFKA_HOST`, etc.) NO se heredan en cluster mode — se pasan como Java system properties via `--conf spark.driver.extraJavaOptions=-DVAR=value`
- El JAR tiene que ser accesible desde los workers, no desde el contenedor submitter

### Por qué el JAR va por HTTP (jar-server)
En `--deploy-mode cluster` el worker descarga el JAR. Se intentó usar MinIO (S3A) pero hay un problema circular:
- `hadoop-aws` necesita `aws-sdk-v2 bundle` (480MB) en el classpath del worker ANTES de descargar el JAR
- Ese bundle no puede venir en `--packages` porque los packages se resuelven DESPUÉS de descargar el JAR
- Solución: `jar-server` es un servidor HTTP Python (`python3 -m http.server 8888`) que sirve el JAR dentro de la red Docker. Spark descarga JARs por HTTP nativamente, sin dependencias extra.

### Archivos en ~/spark-4.1.1/jars/ del host (montados en workers)
Estos JARs se añadieron manualmente porque son necesarios ANTES de que arranque la aplicación:
- `hadoop-aws-3.4.1.jar` — soporte S3A para leer modelos/checkpoints de MinIO en runtime
- `aws-java-sdk-bundle-1.12.262.jar` — AWS SDK v1, requerido por hadoop-aws
- `bundle-2.20.160.jar` fue descargado pero causó crash del worker (conflicto). Eliminar si existe.

---

## Ficheros clave del proyecto

| Fichero | Descripción |
|---|---|
| `docker-compose.yml` | Orquestación de todos los servicios |
| `flight_prediction/entrypoint.sh` | Script bash que hace el spark-submit en cluster mode |
| `flight_prediction/Dockerfile.spark` | Imagen del contenedor spark-predictor (instala mc y netcat) |
| `flight_prediction/src/.../MakePrediction.scala` | Lógica Spark Streaming — NO tiene .master() hardcodeado |
| `resources/web/predict_flask.py` | App Flask con Socket.IO |
| `resources/web/predict_utils.py` | Lee distancias de Cassandra |
| `resources/train_spark_mllib_model.py` | Entrena el modelo Random Forest y lo guarda en MinIO |
| `cassandra-init.sh` | Crea keyspace/tablas e importa distancias al arrancar |

---

## Estado actual (25/05/2026)

### ✅ Funcionando
- 2 workers registrados en Spark Master (Workers: 2 ALIVE)
- `--deploy-mode cluster` operativo: driver se lanza en un worker
- `spark-predictor` sale con código 0 después del submit (comportamiento correcto)
- Flask arranca y sirve la UI
- Cassandra, Kafka, MinIO operativos
- JAR se sirve por `jar-server` (HTTP interno)

### ❌ Problema activo — JAR no encontrado en worker
**Error**: `NoSuchFileException: /app/flight_prediction_2.13-0.1.jar`

**Causa**: El contenedor `spark-predictor` no se ha rebuildeado con el último `entrypoint.sh`. El entrypoint actual en el fichero ya tiene la URL correcta (`http://jar-server:8888/flight_prediction_2.13-0.1.jar`) pero la imagen Docker todavía tiene la versión antigua con `/app/...`.


## Objetivos restantes de la práctica

### Inmediato
- [ ] Resolver el error del JAR (rebuild spark-predictor)
- [ ] Verificar que el streaming funciona end-to-end (submit formulario → predicción en UI)

### Pendiente principal — YARN para entrenamiento
El enunciado requiere que el entrenamiento (`train_spark_mllib_model.py`) use un scheduler.
Se decidió usar YARN (en lugar de Kubernetes) porque no se va a desplegar K8S.

Esto requiere añadir al docker-compose:
- `hadoop-namenode` (HDFS + YARN ResourceManager)
- `hadoop-nodemanager` (YARN NodeManager / ejecutor)
- Archivos de configuración Hadoop: `core-site.xml`, `yarn-site.xml`, `hdfs-site.xml`
- El script de entrenamiento se lanzaría con `--master yarn` en lugar de `local[*]`

### Opcionales (puntos extra según rúbrica)
- [ ] Despliegue en GCloud
- [ ] Observabilidad (métricas, dashboards)
- [ ] Apache Airflow + MLflow para orquestar el entrenamiento

---

## Rúbrica de evaluación (puntos)

| Pts | Requisito | Estado |
|---|---|---|
| 1 | Datos de entrenamiento en MinIO/Iceberg Lakehouse | ✅ |
| 1 | Distancias en Cassandra (no MongoDB) | ✅ |
| 1 | Predicción escrita en Kafka + Cassandra, UI via WebSocket | ✅ |
| 1 | Entrenamiento lee del Lakehouse y guarda modelos en MinIO | ✅ |
| 1 | Dockerizar todo con docker-compose | ✅ (en progreso) |
| 3 | Despliegue completo en K8S | ⬜ |
| 1 | Entrenamiento con Airflow + MLflow en cluster Spark con Docker | ⬜ |
| 1 | Despliegue en GCloud | ⬜ |
| 1 | Mejoras de observabilidad/visualización | ⬜ |

---

## Entorno de desarrollo
- Máquina: ETSIT Labs (Ubuntu) — los contenedores Docker se borran entre sesiones
- El home `/home/mauro.perezc` persiste entre sesiones
- Spark instalado en `~/spark-4.1.1` (montado en workers y predictor)
- Ivy cache en `~/.ivy2` (montado en workers y predictor para packages Maven)
- Modelos ML en MinIO: `s3a://lakehouse/models/`
- Checkpoints Spark en MinIO: `s3a://lakehouse/checkpoints/flight-predictor`
