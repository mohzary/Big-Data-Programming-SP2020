import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //To create a new configuration of spark
    val conf = new SparkConf().
      setMaster("local").
      setAppName("MergeSort")

    // To create spark context sc
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //To create RDD from input dataset file using textfile method
    val RDD = sc.textFile("input/dataset.txt")

    // Split the lines of RDD into words using flatmap() transformation operation:
    val splitRDD = RDD.flatMap(txt => txt.split(" "))

    //Count the total number of words using count() action operation
    val numberOfWords = splitRDD.count()
    println("Total number of words in the dataset file is: ")
    println(numberOfWords)

    //Create a tuple of the word and 1 using Map() transformation operation :
    val wordRDD = splitRDD.map(word => (word, 1))

    //Count of the number of occurences of each word  using reduceByKey() transformation
    val resultRDD = wordRDD.reduceByKey{(x,y) => x + y}

    //Swap the keys and values
    val resultRDD_swap = resultRDD.map(x => (x._2, x._1))

    //Sort the keys in descending order and save output in a text file using sortByKey() transformation operation to deal with pair RDDs,
    // and saveAsTextFile() action operation
    val sortedRDD= resultRDD_swap.sortByKey(false)
    sortedRDD.saveAsTextFile("output/wordCount/")

    //To show the top 10 words mentioned in the text file using take(n) action operation
    //val top10Words = sortedRDD.take(10)
    println("Top 10 words result: ")
    println(sortedRDD.take(10).foreach(println))
  }
}



