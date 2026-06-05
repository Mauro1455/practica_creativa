name := "flight_prediction"
version := "0.1"
scalaVersion := "2.13.0"
val sparkVersion = "4.1.1"
mainClass in Compile := Some("es.upm.dit.ging.predictor.MakePrediction")
resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.cassandra" % "java-driver-core-shaded" % "4.18.1" % "provided"
)
