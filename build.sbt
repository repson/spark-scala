name := "Spark Scala"

version := "1.0"

scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.2"
)
