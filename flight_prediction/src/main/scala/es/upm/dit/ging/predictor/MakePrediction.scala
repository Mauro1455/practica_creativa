package es.upm.dit.ging.predictor
import com.datastax.oss.driver.api.core.CqlSession
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{concat, from_json, lit, to_json}
import org.apache.spark.sql.functions.{struct => sqlStruct}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.net.InetSocketAddress
import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val kafkaHost     = sys.env.getOrElse("KAFKA_HOST",     sys.props.getOrElse("KAFKA_HOST",     "kafka"))
    val minioHost     = sys.env.getOrElse("MINIO_HOST",     sys.props.getOrElse("MINIO_HOST",     "minio"))
    val cassandraHost = sys.env.getOrElse("CASSANDRA_HOST", sys.props.getOrElse("CASSANDRA_HOST", "cassandra"))

    val spark = SparkSession
      .builder
      .appName("FlightDelayPredictor")
      .config("spark.hadoop.fs.s3a.endpoint", s"http://$minioHost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      // Model was saved with Spark 3.x which allowed nullable Int inside Tuple3 tree nodes.
      // Spark 4.x ANSI mode rejects these as NOT_NULL_ASSERT_VIOLATION; disable to load legacy models.
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val base_path = "s3a://lakehouse"

    println("Loading VectorAssembler model...")
    val vectorAssembler = VectorAssembler.load(s"$base_path/models/numeric_vector_assembler.bin")

    println("Loading RandomForest model...")
    val randomForestModelPath = s"$base_path/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    println("Models loaded. Starting Kafka consumer loop...")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    // Kafka consumer
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", s"$kafkaHost:9092")
    consumerProps.put("group.id", "flight-delay-predictor")
    consumerProps.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("auto.offset.reset", "latest")
    consumerProps.put("enable.auto.commit", "false")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(java.util.Arrays.asList("flight-delay-ml-request"))

    // Kafka producer
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", s"$kafkaHost:9092")
    producerProps.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](producerProps)

    val chost = cassandraHost

    println("Polling Kafka for prediction requests...")

    while (true) {
      val records: ConsumerRecords[String, String] =
        consumer.poll(Duration.ofMillis(1000))

      if (!records.isEmpty) {
        val messages = records.asScala.map(_.value()).toSeq
        println(s"Processing batch of ${messages.size} prediction request(s)...")

        try {
          import org.apache.spark.sql.functions.col

          val rawDf = spark.read.schema(struct).json(
            spark.sparkContext.parallelize(messages)
          ).drop("Prediction", "FlightNum")

          val withRoute = rawDf.withColumn(
            "Route",
            concat(rawDf("Origin"), lit('-'), rawDf("Dest"))
          )

          val vectorized = vectorAssembler.setHandleInvalid("keep").transform(withRoute)

          val finalVectorized = vectorized
            .drop("Carrier_index").drop("Origin_index")
            .drop("Dest_index").drop("Route_index")

          val predicted = rfc.transform(finalVectorized)
            .drop("Features_vec").drop("rawPrediction").drop("probability")

          val predCol = predicted.columns.find(_.equalsIgnoreCase("prediction")).getOrElse("prediction")
          val normalised = if (predCol != "prediction") predicted.withColumnRenamed(predCol, "prediction")
                           else predicted

          val collectableDf = normalised.select(
            normalised.columns.map {
              case c if c.equalsIgnoreCase("FlightDate") => col(c).cast("string").as("FlightDate")
              case c if c.equalsIgnoreCase("Timestamp")  => col(c).cast("string").as("Timestamp")
              case other                                  => col(other)
            }: _*
          )

          val resultRows: Array[Row] = collectableDf.collect()
          val jsonCols = collectableDf.columns.map(c => col(c))

          // ── Write predictions to Kafka response topic ──────────────────
          val jsonRows = collectableDf
            .select(to_json(sqlStruct(jsonCols: _*)).as("value"))
            .collect()
            .map(_.getString(0))

          jsonRows.foreach { json =>
            producer.send(new ProducerRecord[String, String]("flight-delay-ml-response", json))
          }
          producer.flush()
          println(s"Sent ${jsonRows.length} prediction(s) to flight-delay-ml-response")

          // ── Write to Cassandra ─────────────────────────────────────────
          try {
            if (resultRows.nonEmpty) {
              val session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(chost, 9042))
                .withLocalDatacenter("datacenter1")
                .withKeyspace("agile_data_science")
                .build()
              try {
                val stmt = session.prepare(
                  "INSERT INTO flight_delay_ml_response " +
                  "(uuid, origin, dest, carrier, dayofweek, dayofyear, dayofmonth, " +
                  "depdelay, distance, prediction, route, flightdate, timestamp) " +
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                )
                resultRows.foreach { row =>
                  session.execute(stmt.bind(
                    row.getAs[String]("UUID"),
                    row.getAs[String]("Origin"),
                    row.getAs[String]("Dest"),
                    row.getAs[String]("Carrier"),
                    Integer.valueOf(row.getAs[Int]("DayOfWeek")),
                    Integer.valueOf(row.getAs[Int]("DayOfYear")),
                    Integer.valueOf(row.getAs[Int]("DayOfMonth")),
                    java.lang.Double.valueOf(row.getAs[Double]("DepDelay")),
                    java.lang.Double.valueOf(row.getAs[Double]("Distance")),
                    java.lang.Double.valueOf(row.getAs[Double]("prediction")),
                    row.getAs[String]("Route"),
                    row.getAs[String]("FlightDate").toString,
                    row.getAs[String]("Timestamp").toString
                  ))
                }
                println(s"Wrote ${resultRows.length} row(s) to Cassandra")
              } finally { session.close() }
            }
          } catch {
            case ex: Exception => println(s"Cassandra write skipped: ${ex.getMessage}")
          }

          consumer.commitSync()

        } catch {
          case ex: Exception =>
            println(s"ERROR processing batch: ${ex.getMessage}")
            ex.printStackTrace()
        }
      }
    }
  }
}
