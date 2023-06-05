ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "CCERS"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2"
