name := "hybrid-json-datasource"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",

  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)