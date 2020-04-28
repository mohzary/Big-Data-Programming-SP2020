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
    val world_Cup_Matches_DF_noDup = world_Cup_Matches_DF.dropDuplicates()
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
    val world_Cup_Players_DF_noDup = world_Cup_Players_DF.dropDuplicates()
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
    val world_Cups_DF_noDUP = world_Cups_DF.dropDuplicates()

    ///=====================================Part(2): To Perform 10 queries=========================================
    //First we create temporary views of data frames
    world_Cup_Matches_DF_noDup.createOrReplaceTempView("matches")
    world_Cup_Players_DF_noDup.createOrReplaceTempView("players")
    world_Cups_DF_noDUP.createOrReplaceTempView("cups")

    //Query(1): To find how many matches USA team wins when it plays as Home team and group number of wins by Away team
    val query1 = spark.sql("SELECT HomeTeamName, AwayTeamName, COUNT(HomeTeamName) as numberOFMatches FROM matches WHERE (((HomeTeamName) == 'USA') AND (HomeTeamGoals > AwayTeamGoals)) GROUP BY AwayTeamName, HomeTeamName ORDER BY numberOFMatches DESC")
    println("Number of matches USA team wins when it plays as home team:")
    query1.show()

    //Query(2) to find out list of matches that played on Camp Nou stadium, one of most famous places in the world
    val query2 = spark.sql("SELECT Datetime, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName FROM matches WHERE Stadium=='Camp Nou' ")
    println("List of matches on Camp Nou stadium:")
    query2.show()

    //Query(3) to find out number of matches for each stadium:
    val query3 = spark.sql("SELECT Stadium, City, COUNT(Stadium) AS numberOfMatches FROM matches GROUP BY Stadium, City ORDER BY numberOfMatches DESC")
    println("Number of Matches for each Stadium:")
    query3.show()

    //Query(4) To find out matches Brazil and Germany national teams played togather
    val query4 = spark.sql("SELECT Datetime, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName FROM matches WHERE ((HomeTeamName=='Brazil' OR HomeTeamName=='Germany') AND (AwayTeamName=='Brazil' OR AwayTeamName=='Germany'))")
    println("Details of Brazil and Germany national teams matches")
    query4.show()

    //Query(5) To find out match with maximum number of attendance
    val query5 = spark.sql("SELECT MatchID, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName, MAX(Attendance) AS Attendance FROM matches GROUP BY MatchID, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName ORDER BY Attendance DESC LIMIT 1")
    println("The match with maximum number of attendance is:")
    query5.show()

    //Query(6) To find out the average number of Attendance for each stadium
    val query6 = spark.sql("SELECT Stadium, City, Avg(Attendance) AS average FROM matches GROUP BY Stadium, City ORDER BY average DESC")
    println("Average number of attendance")
    query6.show()

    //Query(7) To find out match with minimum number of attendance
    val query7 = spark.sql("SELECT MatchID, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName, MIN(Attendance) AS Attendance FROM matches GROUP BY MatchID, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName ORDER BY Attendance ASC")
    println("The match with minimum number of attendance is:")
    query7.show()

    //Query(8) To Find out how many times city hosted the final of the world cup
    val query8 = spark.sql("SELECT CITY, Stage, COUNT(City) AS Total FROM matches WHERE Stage=='Final' GROUP BY City, Stage ORDER BY Total DESC")
    println("Total of how many times city hosted final match")
    query8.show()

    //Query(9) To find out list of stadium that hosted final matches
    val query9 = spark.sql("SELECT Stadium, City, Stage FROM matches where Stage=='Final' GROUP BY City, Stadium, Stage")
    println("List of stadium hosted final matches in the world cup")
    query9.show()

    //Query(10) To find out a list of final matches and show some details about each match
    val query10 = spark.sql("SELECT Datetime, Stage, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName FROM matches WHERE Stage=='Final' GROUP BY Stage, Stadium, City, HomeTeamName, HomeTeamGoals, AwayTeamGoals, AwayTeamName, Datetime")
    println("List of Final Matches:")
    query10.show()

    //-------------------------Part(3): Perform any 5 queries in Spark RDD’s and Spark Data Frames-------------------
    // To create a new RDD from worldcups dataset
    val RDD = sc.textFile("input/WorldCups.csv")
    val header = RDD.first()
    val world_cups_RDD = RDD.filter(line => line != header)

    // Query(1): To find out Country hosted World Cup in 2014 using RDD
    val query1_rdd = world_cups_RDD.filter(line => line.split(",")(0) == "2014").map(line => (line.split(",")(0), (line.split(",")(1))) )
    println("Country hosted World Cup in 2014 is: (RDD Result)\n")
    query1_rdd.foreach(println)

    println("\nCountry hosted World Cup in 2014 is: (Data Frame Result)\n")
    world_Cups_DF_noDUP.select("Year", "Country").filter("Year = 2014").show()

    //Query(2): To find a list of winner teams and year
    val query2_rdd = world_cups_RDD.map(line => (line.split(",")(0), (line.split(",")(2))) )
    println("list of winner teams and year: (RDD Result)\n")
    query2_rdd.foreach(println)

    println("\nlist of winner teams and year: (Data Frame Result)\n")
    world_Cups_DF_noDUP.select("Year", "Winner").show()

    // Query(3): To find out list of World Cups where GoalsScored is greater than 150
    val query3_rdd = world_cups_RDD.filter(line => line.split(",")(6) > "150").map(line => (line.split(",")(0), line.split(",")(6), (line.split(",")(1))) )
    println("list of World Cups where GoalsScored is greater than 150: (RDD Result)\n")
    query3_rdd.foreach(println)

    println("\nlist of World Cups where GoalsScored is greater than 150: (Data Frame Result)\n")
    world_Cups_DF_noDUP.select("Year", "GoalsScored", "Country").filter("GoalsScored > 150").show()

    // Query(4): To find out list of World Cups where Winner is Brazil and the Runners-up is Germany
    val query4_rdd = world_cups_RDD.filter(line => line.split(",")(2) == "Brazil" && line.split(",")(3) == "Germany").map(line => (line.split(",")(0), line.split(",")(2), (line.split(",")(3))) )
    println("list of World Cups where Winner is Brazil and the Runners-up is Germany: (RDD Result)\n")
    query4_rdd.foreach(println)

    println("\nlist of World Cups where Winner is Brazil and the Runners-up is Germany: (Data Frame Result)\n")
    world_Cups_DF_noDUP.select("Year", "GoalsScored", "Runners-Up").filter("Winner=='Brazil' AND 'Runners-Up'=='Germany'").show()

    //Query(5) To show how many matches had Argentina played and won
    val query5_rdd = world_cups_RDD.filter(line => (line.split(",")(2)=="Argentina" ))
      .map(line => (line.split(",")(0), line.split(",")(1), line.split(",")(2), line.split(",")(3), line.split(",")(4), line.split(",")(5), line.split(",")(6), line.split(",")(7), line.split(",")(8), line.split(",")(9))).collect()
    println("To show how many matches had Argentina played and won: (RDD Result)\n")
    query5_rdd.foreach(println)
    println("\nTo show how many matches had Argentina played and won: (Data Frame Result)\n")
    world_Cups_DF_noDUP.select("*").filter("Country == 'Argentina'").show(2)

  }
}