import org.apache.spark.{SparkConf, SparkContext}

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

    // Split the records in the RDD using map transformation operation and split function
    val splitRDD = RDD.map(data => data.split(","))

    //Create a tuple of the key value pairs using Map() transformation operation :
    //val pairsRDD = RDD.map(_.split(",")).map { k => (k(0), k(1)) }

    //val numReducers = 2;

    //val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))

    //val resultRDD = listRDD.flatMap {
     // case (label, list) => {
      //  list.map((label, _))
     // }



  }
}
