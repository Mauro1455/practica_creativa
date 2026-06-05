package es.upm.dit.ging.predictor
import com.datastax.oss.driver.api.core.CqlSession
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{concat, from_json, lit, to_json}
import org.apache.spark.sql.functions.{struct => sqlStruct}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.net.InetSocketAddress

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
      .getOrCreate()

    import spark.implicits._

    val base_path = "s3a://lakehouse"
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
  }
}
