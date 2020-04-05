name := "SecondarySorting"

version := "0.1"

scalaVersion := "2.11.12"

//libraryDependencies += "org.apache.spark" %% "spark-core" %  "2.4.4"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)