import org.apache.spark._

object SecondarySorting {
  def main(args: Array[String]): Unit = {
    //To create a new configuration of spark
    val conf = new SparkConf().
      setMaster("local").
      setAppName("SecondarySorting")

    // To create spark context sc
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //To create RDD from input dataset file using textfile method
    val RDD = sc.textFile("input/dataset.txt")


    //To create a new pair RDD from the existing RDD we used map() and split() methods forst to split data using ","
    // then we map output of split method into key value pairs
    val pairRDD =RDD.map(_.split(",")).map{k => (k(0),k(1))}

    //to define the number of partitions we want in the reducing function
    val numReducers = 2;

    //To group pairs RDD by key and sort them using value instead of using key
    val listRDD = pairRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))

    //To use flat map to return a list of multiples values for each element in the original RDD
    val resultRDD =listRDD.flatMap{
      case (labe1,list) => {

        list.map((labe1, _))
      }
    }

    //to save the output into text file in the output folder
    resultRDD.saveAsTextFile("output/")
  }
}
