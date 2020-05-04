name := "Graph_Code"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
val graphframesVersion = "0.5.0-spark2.1-s_2.11"

resolvers ++= Seq(
  "bintray-artifacts" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "graphframes" % "graphframes" % graphframesVersion
)