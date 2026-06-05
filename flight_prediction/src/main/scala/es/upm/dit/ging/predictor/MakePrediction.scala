package es.upm.dit.ging.predictor
import com.datastax.oss.driver.api.core.CqlSession
<<<<<<< HEAD
=======
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
>>>>>>> f053088 (Update files)
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{concat, from_json, lit, to_json}
import org.apache.spark.sql.functions.{struct => sqlStruct}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.net.InetSocketAddress
<<<<<<< HEAD
=======
import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._
>>>>>>> f053088 (Update files)

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
<<<<<<< HEAD
=======
      // Model was saved with Spark 3.x which allowed nullable Int inside Tuple3 tree nodes.
      // Spark 4.x ANSI mode rejects these as NOT_NULL_ASSERT_VIOLATION; disable to load legacy models.
      .config("spark.sql.ansi.enabled", "false")
>>>>>>> f053088 (Update files)
      .getOrCreate()

    import spark.implicits._

    val base_path = "s3a://lakehouse"
<<<<<<< HEAD
    val vectorAssembler = VectorAssembler.load(s"$base_path/models/numeric_vector_assembler.bin")

    val randomForestModelPath = s"$base_path/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$kafkaHost:9092")
      .option("subscribe", "flight-delay-ml-request")
      .load()
    df.printSchema()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")
=======
    println("Loading VectorAssembler model...")
    val vectorAssembler = VectorAssembler.load(s"$base_path/models/numeric_vector_assembler.bin")

    println("Loading RandomForest model...")
    val randomForestModelPath = s"$base_path/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    println("Models loaded. Starting Kafka consumer loop...")
>>>>>>> f053088 (Update files)

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

<<<<<<< HEAD
    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))

    val flightFlattenedDf = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek", "flight.DayOfYear", "flight.DayOfMonth", "flight.Dest",
      "flight.DepDelay", "flight.Timestamp", "flight.FlightDate",
      "flight.Carrier", "flight.UUID", "flight.Distance",
      "flight.Carrier_index", "flight.Origin_index", "flight.Dest_index", "flight.Route_index")

    val predictionRequestsWithRoute = flightFlattenedDf.withColumn(
      "Route",
      concat(flightFlattenedDf("Origin"), lit('-'), flightFlattenedDf("Dest"))
    )

    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRoute)

    val finalVectorizedFeatures = vectorizedFeatures
      .drop("Carrier_index")
      .drop("Origin_index")
      .drop("Dest_index")
      .drop("Route_index")

    val predictions = rfc.transform(finalVectorizedFeatures).drop("Features_vec")
    val finalPredictions = predictions.drop("rawPrediction").drop("probability")

    val chost = cassandraHost

    val query = finalPredictions
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val cols = batchDF.columns.map(c => batchDF(c))
        batchDF.select(to_json(sqlStruct(cols: _*)).as("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", s"$kafkaHost:9092")
          .option("topic", "flight-delay-ml-response")
          .save()

        batchDF
          .select(
            $"UUID".as("uuid"), $"Origin".as("origin"), $"Dest".as("dest"),
            $"Carrier".as("carrier"), $"DayOfWeek".as("dayofweek"),
            $"DayOfYear".as("dayofyear"), $"DayOfMonth".as("dayofmonth"),
            $"DepDelay".as("depdelay"), $"Distance".as("distance"),
            $"prediction".as("prediction"), $"Route".as("route"),
            $"FlightDate".cast("string").as("flightdate"),
            $"Timestamp".cast("string").as("timestamp")
          )
          .foreachPartition { (iter: Iterator[Row]) =>
            val rows = iter.toArray
            if (rows.nonEmpty) {
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
                rows.foreach { row =>
                  session.execute(stmt.bind(
                    row.getAs[String]("uuid"),
                    row.getAs[String]("origin"),
                    row.getAs[String]("dest"),
                    row.getAs[String]("carrier"),
                    Integer.valueOf(row.getAs[Int]("dayofweek")),
                    Integer.valueOf(row.getAs[Int]("dayofyear")),
                    Integer.valueOf(row.getAs[Int]("dayofmonth")),
                    java.lang.Double.valueOf(row.getAs[Double]("depdelay")),
                    java.lang.Double.valueOf(row.getAs[Double]("distance")),
                    java.lang.Double.valueOf(row.getAs[Double]("prediction")),
                    row.getAs[String]("route"),
                    row.getAs[String]("flightdate"),
                    row.getAs[String]("timestamp")
                  ))
                }
              } finally {
                session.close()
              }
            }
          }
      }
      .option("checkpointLocation", s"s3a://lakehouse/checkpoints/flight-predictor")
      .start()

    query.awaitTermination()
=======
    // Kafka consumer
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", s"$kafkaHost:9092")
    consumerProps.put("group.id", "flight-delay-predictor")
    consumerProps.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("auto.offset.reset", "latest")   // only new messages; no reprocessing on restart
    consumerProps.put("enable.auto.commit", "false")  // commit explicitly after each batch

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

    // Main poll loop — runs on the cluster driver (--deploy-mode cluster).
    // Uses native Kafka consumer + Spark batch processing to avoid the
    // DataSource V2 / DefaultSerializationProxy serialization bug in Spark 4.1.1.
    while (true) {
      val records: ConsumerRecords[String, String] =
        consumer.poll(Duration.ofMillis(1000))

      if (!records.isEmpty) {
        val messages = records.asScala.map(_.value()).toSeq
        println(s"Processing batch of ${messages.size} prediction request(s)...")

        try {
          import org.apache.spark.sql.functions.col

          // Drop the input placeholder 'Prediction' field before rfc.transform adds its own.
          // The model output column may be 'prediction' or 'Prediction' depending on how
          // the model was saved; we normalise to 'prediction' (lowercase) below.
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

          // Normalise the prediction column to lowercase 'prediction' regardless of
          // how the model was originally saved (avoids case-mismatch at collect time).
          val predCol = predicted.columns.find(_.equalsIgnoreCase("prediction")).getOrElse("prediction")
          val normalised = if (predCol != "prediction") predicted.withColumnRenamed(predCol, "prediction")
                           else predicted

          // Cast temporal columns to String to avoid Java 17 sun.util.calendar access
          // issues in Spark codegen when decoding DateType/TimestampType on the driver.
          val collectableDf = normalised.select(
            normalised.columns.map {
              case c if c.equalsIgnoreCase("FlightDate") => col(c).cast("string").as("FlightDate")
              case c if c.equalsIgnoreCase("Timestamp")  => col(c).cast("string").as("Timestamp")
              case other                                  => col(other)
            }: _*
          )

          // Collect once; reuse for Kafka write and Cassandra write.
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

          // ── Write to Cassandra (best-effort; does not block Kafka write) ──
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

          // Commit Kafka offsets explicitly so restarts don't reprocess old messages.
          consumer.commitSync()

        } catch {
          case ex: Exception =>
            println(s"ERROR processing batch: ${ex.getMessage}")
            ex.printStackTrace()
        }
      }
    }
>>>>>>> f053088 (Update files)
  }
}
