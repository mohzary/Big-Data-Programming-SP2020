import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes._

object Graph_Code{
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

    //=====================================Task 1========================================================
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
    //To re-name Columns
    // I used withColumnRenamed() method to re-name some columns name in the two data frames
    val station_data = station_data_RDD.withColumnRenamed("station_id", "id")
    val trip_data = trip_data_RDD.withColumnRenamed("Start Terminal", "src").withColumnRenamed("End Terminal", "dst").withColumnRenamed("Trip ID", "trip")
    //To show dataFrame schema and the first 5 rows:
    println("Station DataFrame Schema:")
    station_data.printSchema()
    println("The first 5 rows in the station dataFrame:")
    station_data.show(5)
    //To show dataFrame schema and the first 5 rows for second file:
    println("Trip DataFrame Schema:")
    trip_data.printSchema()
    println("The first 5 rows in the trip dataFrame:")
    trip_data.show(5)

    //To create graph out of data frames
    //Create vertices
    //First, we create two temporary views of data frames to use with spark sql
    station_data.createOrReplaceTempView("stations")
    trip_data.createOrReplaceTempView("trips")

    //to create vertices data frame
    val v = spark.sql("SELECT id, name, landmark FROM stations")

    //to create edges data frame
    val e = spark.sql("SELECT src, dst, trip FROM trips")


    //To create graph out of dataframes
    val stations_trips_graph = GraphFrame(v , e)

    println("To check out if the graph is created: ")
    println(stations_trips_graph)

    //=====================================Task 2========================================================
    //Triangle Count

    val triangle_Count = stations_trips_graph.triangleCount.run()
    println("Triangle Count Result: ")
    triangle_Count.show()

    //=====================================Task 3========================================================
    //Find Shortest Paths w.r.t. Landmarks
    val shortest_path = stations_trips_graph.bfs.fromExpr("id = 68").toExpr("id = 76").run()
    println("Shortest Paths result")
    shortest_path.show()
    //=====================================Task 4========================================================
    // Apply Page Rank algorithm on the dataset.
    val page_rank = stations_trips_graph.pageRank.resetProbability(0.15).tol(0.01).run()

    println("Resulting pageranks")
    page_rank.vertices.select("id", "pagerank").show()

    println("final edge weights")
    page_rank.edges.select("src", "dst", "weight").show()

    //=====================================Task 5========================================================
    //Save graphs generated to a file
    // Save vertices and edges as Parquet to some location.
    stations_trips_graph.vertices.write.parquet("vertices_output/vertices")
    stations_trips_graph.edges.write.parquet("edges_output/edges")

    //=====================================Bonus========================================================

    //1. Apply Label Propagation Algorithm
    val label_Propagation = stations_trips_graph.labelPropagation.maxIter(5).run()
    println("Result of Label Propagation Algorithm: ")
    label_Propagation.select("id", "label").show()

    //2. Apply BFS algorithm
    val paths = stations_trips_graph.bfs.fromExpr("id = 68").toExpr("id = 70").run()
    println("Shortest path using BFS algorithm:")
    paths.show()




  }
}