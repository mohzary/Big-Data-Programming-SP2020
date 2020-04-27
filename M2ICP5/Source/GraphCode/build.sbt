name := "GraphCode"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-graphx" % "2.4.0",
  "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
)



