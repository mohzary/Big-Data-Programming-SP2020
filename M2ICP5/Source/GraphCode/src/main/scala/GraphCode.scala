import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.array
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.graphx._

import org.graphframes.GraphFrame

object GraphCode{
  def main(args: Array[String]): Unit = {

    //To create a new configuration of spark
    val conf = new SparkConf().
      setMaster("local").
      setAppName("GraphCode")

    // To create spark context sc
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //To create a sparkSession called spark
    val spark = {
      SparkSession.builder().appName("GraphCode").config("spark.master", "local").getOrCreate()
    }
    //=====================================Part(1)(1)========================================================
    //To Import the dataset as a csv file and create data frames directly on import
    //I used read method to import and create dataFrame
    val station_data_RDD = spark.read
      .format("org.apache.spark.csv")
      .option("header", true) // to automatically columns name from the dataset file
      .option("inferSchema", true) //  to automatically recognize the type of the variable
      .csv("input/201508_station_data.csv")


    //To import second file
    val trip_data_RDD = spark.read
      .format("org.apache.spark.csv")
      .option("header", true) // to automatically columns name from the dataset file
      .option("inferSchema", true) //  to automatically recognize the type of the variable
      .csv("input/201508_trip_data.csv")

    //To show dataFrame schema and the first 5 rows:
    println("Station DataFrame Schema:")
    station_data_RDD.printSchema()
    println("The first 5 rows in the station dataFrame:")
    station_data_RDD.show(5)

    //To show dataFrame schema and the first 5 rows for second file:
    println("Trip DataFrame Schema:")
    trip_data_RDD.printSchema()
    println("The first 5 rows in the trip dataFrame:")
    trip_data_RDD.show(5)

    //=====================================Part(1)(2)========================================================
    //To Concatenate chunks into list & convert to Data Frame

    val station_data_RDD2 = station_data_RDD.withColumn("geo_data", array("lat", "long"))
    println("New station dataframe including geo_data column:")
    station_data_RDD2.show(5)

    //=====================================Part(1)(3)========================================================
    //Remove duplicates
    //Check for Duplicate records in the two dataframes, I used dropDuplicates() to remove duplicate records:
    val station_data_RDD_NoDUP = station_data_RDD.dropDuplicates()
    val trip_data_RDD_NoDUP = trip_data_RDD.dropDuplicates()

    //To show number of records before and after removing duplicate records, I used count() action method to return number of records
    println("Number of rows before removing duplicates from station dataframe:")
    println(station_data_RDD.count())
    println("Number of rows after removing duplicates from station dataframe:")
    println(station_data_RDD_NoDUP.count())

    println("Number of rows before removing duplicates from trip dataframe:")
    println(trip_data_RDD.count())
    println("Number of rows after removing duplicates from trip dataframe:")
    println(trip_data_RDD_NoDUP.count())

    //=====================================Part(1)(4)========================================================
    //Name Columns
    // I used withColumnRenamed() method to re-name some columns name in the two data frames
    val station_data = station_data_RDD.withColumnRenamed("station_id", "id")
    val trip_data = trip_data_RDD.withColumnRenamed("Start Terminal", "src").withColumnRenamed("End Terminal", "dst").withColumnRenamed("Trip ID", "trip")

    //====================================Part(1)(5)========================================================
    //Output Data Frame
    println("Stations data frame with new column name:")
    station_data.show(5)

    println("Trips data frame with new columns names:")
    trip_data.show(5)

    //====================================Part(1)(6)========================================================
    //Create vertices

    //First, we create two temporary views of data frames to use with spark sql
    station_data.createOrReplaceTempView("stations")
    trip_data.createOrReplaceTempView("trips")

    //to create vertices data frame
    val v = spark.sql("SELECT id FROM stations")

    //to create edges data frame
    val e = spark.sql("SELECT src, dst FROM trips")

    val stations_trips_graph = {
      Graph(v.count(),e.count())
    }

    //====================================Part(1)(7)========================================================
    //Show some edges
    println("An example of graph vertices: " + stations_trips_graph.vertices)
    //====================================Part(1)(8)========================================================
    //Show some edges
    println("An example of graph edges: " + stations_trips_graph.edges)
  }
}

