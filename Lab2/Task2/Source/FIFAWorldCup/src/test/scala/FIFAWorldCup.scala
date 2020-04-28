import org.apache.spark._
import org.apache.spark.sql.SparkSession
object FIFAWorldCup {
  def main(args: Array[String]): Unit = {

    //To create a new configuration of spark
    val conf = new SparkConf().
      setMaster("local").
      setAppName("FIFAWorldCup")

    // To create spark context sc
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //To create a sparkSession called spark
    val spark = {
      SparkSession.builder().appName("FIFAWorldCup").config("spark.master", "local").getOrCreate()
    }

    //=====================================Part(1): To Import datasets=========================================
    //To import the dataset and create data frames directly on import
    //I used read method to import and create dataFrame from WorldCupMatches.csv file
    val world_Cup_Matches_DF = spark.read
      .format("org.apache.spark.csv")
      .option("header", true) // to automatically columns name from the dataset file
      .option("inferSchema", true) //  to automatically recognize the type of the variable
      .csv("input/WorldCupMatches.csv")

    //To show dataFrame schema and the first 5 rows:
    println("World Cup Matches DataFrame Schema:")
    world_Cup_Matches_DF.printSchema()
    println("The first 5 rows in the World Cup Matches dataFrame:")
    world_Cup_Matches_DF.show(5)
    //****************************************
    //I used read method to import and create dataFrame from WorldCupPlayers.csv file
    val world_Cup_Players_DF = spark.read
      .format("org.apache.spark.csv")
      .option("header", true) // to automatically columns name from the dataset file
      .option("inferSchema", true) //  to automatically recognize the type of the variable
      .csv("input/WorldCupPlayers.csv")
    //To show dataFrame schema and the first 5 rows:
    println("World Cup Players DataFrame Schema:")
    world_Cup_Players_DF.printSchema()
    println("The first 5 rows in the World Cup Players dataFrame:")
    world_Cup_Players_DF.show(5)
    //**************************************************
    //I used read method to import and create dataFrame from WorldCups.csv file
    val world_Cups_DF = spark.read
      .format("org.apache.spark.csv")
      .option("header", true) // to automatically columns name from the dataset file
      .option("inferSchema", true) //  to automatically recognize the type of the variable
      .csv("input/WorldCups.csv")
    //To show dataFrame schema and the first 5 rows:
    println("World Cups DataFrame Schema:")
    world_Cups_DF.printSchema()
    println("The first 5 rows in the World Cups dataFrame:")
    world_Cups_DF.show(5)

    ///=====================================Part(2): To Perform 10 queries=========================================
    //First we create temporary views of data frames
    world_Cup_Matches_DF.createOrReplaceTempView("matches")
    world_Cup_Players_DF.createOrReplaceTempView("players")
    world_Cups_DF.createOrReplaceTempView("cups")

    //Query(1): To find how many matches USA team wins when it plays as Home team and group number of wins by Away team
    //val query1 = spark.sql("SELECT 'Home Team Name', 'Away Team Name', COUNT('Home Team Name') as numberOFMatches FROM matches WHERE ((('Home Team Name') == 'USA') AND ('Home Team Goals' > 'Away Team Goals')) GROUP BY 'Away Team Name' ORDER BY numberOFMatches DESC")
    val query1 = spark.sql("SELECT 'Home Team Name' AS HomeTeam, 'Away Team Name' FROM matches WHERE ((HomeTeam) == 'USA') ")

    println("Number of matches USA team wins when it plays as home team:")
    query1.show()







  }
}