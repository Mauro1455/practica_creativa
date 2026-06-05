# Guía de arranque - Práctica Creativa (ETSIT Labs)

Pasos para levantar todo después de reiniciar el ordenador.

> ⚠️ **ETSIT labs**: los contenedores Docker se borran entre sesiones. Hay que recrearlos cada vez.
> El home (`/home/mauro.perezc`) persiste, por lo que los modelos de MinIO, el código y los datos están siempre disponibles.

---

## 0. Verificar versiones y variables de entorno

```bash
sdk use java 17.0.14-amzn
export SPARK_HOME=~/spark-4.1.1
export PATH=$SPARK_HOME/bin:$PATH
spark-submit --version
# Esperado: version 4.1.1 / Scala 2.13.17
```

---

## 1. Crear y arrancar Cassandra

Los contenedores se borran entre sesiones, hay que recrearlo cada vez:

```bash
docker rm -f cassandra 2>/dev/null; docker run --name cassandra -d -p 9042:9042 cassandra:4.1
```

Espera 30 segundos y verifica que está listo:
```bash
docker logs cassandra | tail -5
# Espera hasta ver: Starting listening for CQL clients
```

Luego crea el keyspace e importa las distancias (**obligatorio cada sesión**):

```bash
docker exec -it cassandra cqlsh
```

```sql
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE agile_data_science;

CREATE TABLE IF NOT EXISTS origin_dest_distances (
    Origin text,
    Dest text,
    Distance double,
    PRIMARY KEY (Origin, Dest)
);

CREATE TABLE IF NOT EXISTS flight_delay_ml_response (
    uuid text PRIMARY KEY,
    origin text,
    dest text,
    carrier text,
    dayofweek int,
    dayofyear int,
    dayofmonth int,
    depdelay double,
    distance double,
    prediction double,
    flightdate text,
    timestamp text,
    route text
);

exit
```

```bash
cd ~/practica_creativa
source env/bin/activate

python3 - <<'EOF'
import json
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('agile_data_science')

with open('data/origin_dest_distances.jsonl') as f:
    for line in f:
        doc = json.loads(line)
        session.execute(
            "INSERT INTO origin_dest_distances (Origin, Dest, Distance) VALUES (%s, %s, %s)",
            (doc['Origin'], doc['Dest'], float(doc['Distance']))
        )

print("Importación completada")
cluster.shutdown()
EOF
```

✅ Listo cuando veas: `Importación completada`

---

## 2. Crear y arrancar MinIO

Los datos persisten en `~/practica_creativa/data_minio` pero el contenedor hay que recrearlo:

```bash
docker rm -f minio 2>/dev/null; docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -v ~/practica_creativa/data_minio:/data \
  -e MINIO_ROOT_USER=minio \
  -e MINIO_ROOT_PASSWORD=minio123 \
  minio/minio server /data --console-address ":9001"
```

Verifica que los modelos siguen ahí:
```bash
docker exec minio mc alias set local http://localhost:9000 minio minio123
docker exec minio mc ls local/lakehouse/models/
```

✅ Listo cuando veas los 7 modelos listados.

> Si los modelos no aparecen, hay que entrenar de nuevo. Ver apartado **"Entrenar el modelo"** al final.

---

## 3. Arrancar Kafka

```bash
cd ~/Descargas/kafka_2.13-4.2.0
chmod +x bin/*.sh

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

✅ Listo cuando veas: `Kafka Server started`

**Deja esta terminal abierta.**

---

## 4. Crear los topics de Kafka

En una terminal nueva:

```bash
cd ~/Descargas/kafka_2.13-4.2.0

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 \
  --topic flight-delay-ml-request

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 \
  --topic flight-delay-ml-response
```

Verifica:
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

✅ Listo cuando veas ambos topics listados.

---

## 5. Lanzar Spark Streaming (Flight Predictor)

En una terminal nueva:

```bash
cd ~/practica_creativa/flight_prediction
export SPARK_HOME=~/spark-4.1.1
export PATH=$SPARK_HOME/bin:$PATH

rm -rf /tmp/checkpoint-multi

spark-submit \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,\
org.apache.hadoop:hadoop-aws:3.4.1,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
com.datastax.spark:spark-cassandra-connector_2.13:3.5.1 \
  target/scala-2.13/flight_prediction_2.13-0.1.jar
```

✅ Listo cuando veas mensajes de INFO continuos sin errores.

**Deja esta terminal abierta.**

---

## 6. Lanzar la aplicación web Flask

En una terminal nueva:

```bash
cd ~/practica_creativa/resources/web
source ~/practica_creativa/env/bin/activate
export PROJECT_HOME=/home/mauro.perezc/practica_creativa
python3 predict_flask.py
```

✅ Listo cuando veas: `Running on http://127.0.0.1:5001`

---

## 7. Probar

Abre en el navegador:

```
http://localhost:5001/flights/delays/predict_kafka
```

Rellena el formulario y envía una predicción. Debería aparecer el resultado en segundos via WebSocket.

Verifica que se guardó en Cassandra:
```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM agile_data_science.flight_delay_ml_response LIMIT 3;"
```

---

## Resumen de terminales

| Terminal | Servicio | Cuándo cerrar |
|----------|----------|---------------|
| 1 | Kafka | Al terminar |
| 2 | Spark Streaming | Al terminar |
| 3 | Flask | Al terminar |

> Cassandra y MinIO corren en Docker y no necesitan terminal dedicada.
> MongoDB ya no es necesario.

---

## Errores frecuentes

| Error | Causa | Solución |
|-------|-------|----------|
| `UnsupportedClassVersionError` | Java 8 activo en lugar de Java 17 | `sdk use java 17.0.14-amzn` y exportar `JAVA_HOME` |
| `No readable meta.properties files found` | Kafka sin formatear tras reinicio | Ejecutar pasos de formato del apartado 3 |
| `NoSuchFileException: config/kraft/server.properties` | En Kafka 4.x el fichero está en `config/server.properties` | Usar `config/server.properties` con flag `--standalone` |
| `Permiso denegado` en scripts de Kafka | Scripts sin permisos de ejecución | `chmod +x bin/*.sh` |
| `UnknownTopicOrPartitionException` | Topic no creado | Ejecutar apartado 4 |
| `NoHostAvailable: agile_data_science` | Keyspace de Cassandra no creado | Ejecutar apartado 1 completo |
| `ImportError: cannot import name 'KafkaProducer'` | Conflicto de librerías kafka | `pip uninstall kafka kafka-python kafka-python-ng -y && pip install kafka-python-ng` |
| `UnsupportedVersionException: api version 2` | Cliente Kafka Python con api_version antigua | Cambiar `api_version=(0,10)` por `api_version=(4,2,0)` en `predict_flask.py` |
| `Connection to localhost:9092 could not be established` | Kafka no está corriendo | Arrancar Kafka antes que Spark |
| `NumberFormatException: For input string: "60s"` | Incompatibilidad hadoop-aws 3.3.4 con Spark 4.1.1 | Usar `hadoop-aws:3.4.1` en el spark-submit |
| `SparkContext stopped` tras pocos segundos | Error en foreachBatch | Borrar checkpoint: `rm -rf /tmp/checkpoint-multi` y relanzar |

---

## Entrenar el modelo (solo si se pierden los modelos de MinIO)

```bash
cd ~/practica_creativa
source env/bin/activate
export SPARK_HOME=~/spark-4.1.1
export PATH=$SPARK_HOME/bin:$PATH

spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,\
org.apache.hadoop:hadoop-aws:3.4.1,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  resources/train_spark_mllib_model.py .
```

✅ Correcto si ves: `✅ Model saved to MinIO` y `Accuracy = 0.58...`
