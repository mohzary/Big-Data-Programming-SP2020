import org.apache.spark._
import org.apache.spark.sql.SparkSession
object Survey {
  def main(args: Array[String]): Unit = {

    //To create a new configuration of spark
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Survey")

    // To create spark context sc
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //To create a sparkSession called spark
    val spark = {
      SparkSession.builder().appName("Survey").config("spark.master", "local").getOrCreate()
    }
    //=====================================Part(1)(1)========================================================
    //To import the dataset and create data frames directly on import
    //I used read method to import and create dataFrame from survey.csv file
    val survey_DF = spark.read
      .format("org.apache.spark.csv")
      .option("header", true) // to automatically columns name from the dataset file
      .option("inferSchema", true) //  to automatically recognize the type of the variable
      .csv("input/survey.csv")

    //To show dataFrame schema and the first 5 rows:
    println("Survey DataFrame Schema:")
    survey_DF.printSchema()
    println("The first 5 rows in the survey dataFrame:")
    survey_DF.show(5)

    //====================================Part(1)(2)==========================================================
    //Save dataFrame to file
    survey_DF.write.format("com.databricks.spark.csv").save("output/survey_DF/survey_DF.csv")

    //====================================Part(1)(3)==========================================================

    //Check for Duplicate records in the dataset, I used dropDuplicates() to remove duplicate records:
    val survey_DF_noDup = survey_DF.select("Timestamp", "Age", "Gender", "Country", "state").dropDuplicates()
    println("Number of rows before removing duplicates")
    //To show number of records before and after removing duplicate records, I used count() action method to return number of records
    println(survey_DF.count())
    println("Number of rows after removing duplicates")
    println(survey_DF_noDup.count())

    //====================================Part(1)(4)==========================================================
    //Apply Union operation on the dataset and order the output by CountryName alphabetically.

    //First, I split survey_df, I used randomSplit() method
    val survey_DF_Split = survey_DF.randomSplit(Array(1,1))
    val survey_DF_Split1 =  survey_DF_Split(0)
    val survey_DF_Split2 = survey_DF_Split(1)

    //Second, I apply union operation on dataframes
    val survey_DF_Union = survey_DF_Split1.union(survey_DF_Split2)

    //Third, I used orderBy() method to order the output by country Name alphabetically
    val survey_DF_Union_OrderBy =  survey_DF_Union.orderBy("Country")
    println("The output ordered by CountryName alphabetically: ")
    survey_DF_Union_OrderBy.show(20)

    //Finally, I save output into new file
    survey_DF_Union_OrderBy.write.format("com.databricks.spark.csv").save("output/Union-orderBy-Country/union.csv")

    //====================================Part(1)(5)==========================================================
    //Use groupBy Query based on treatment.

    //First, I create a new view from dataframe, to execute SQL queries
    survey_DF.createOrReplaceTempView("survey")
    val query1_DF = spark.sql("SELECT treatment, COUNT(treatment) AS numberOfResponses from survey GROUP BY treatment")

    println("Use groupBy Query based on treatment result:")
    query1_DF.show(10)

    //====================================Part(2)(1)==========================================================
    //Apply the basic queries related to Joins and aggregate functions(at least 2)
    val query2_DF = spark.sql("SELECT Country, MAX(Age) AS maxAge FROM survey GROUP BY Country ORDER BY maxAge DESC")
    println("Maximum age of participants per country")
    query2_DF.show()
    //I save output into new file
    query2_DF.write.format("com.databricks.spark.csv").save("output/queries/query2.csv")

    val query3_DF = spark.sql("SELECT state, Age, COUNT(Age) as numberOfCasesPerAge FROM survey WHERE " +
      "(Gender=='Female' OR Gender=='F' OR Gender=='female' OR Gender=='f') AND (seek_help=='Yes') " +
      "GROUP BY state, Age ORDER BY numberOfCasesPerAge DESC")

    println("Number of cases seeking for help per Age and State")
    query3_DF.show()
    //I save output into new file
    query3_DF.write.format("com.databricks.spark.csv").save("output/queries/query3.csv")


    //====================================Part(2)(2)==========================================================
    //Write a query to fetch 13th Row in the dataset. 
    val query4_DF = spark.sql("SELECT * FROM survey LIMIT 13")
    println("To return 13th Rows from the dataset")
    query4_DF.show()
    //I save output into new file
    query4_DF.write.format("com.databricks.spark.csv").save("output/queries/query4.csv")

  }
}