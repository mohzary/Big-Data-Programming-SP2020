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

    val pairRDD =RDD.map(_.split(",")).map{k => (k(0),k(1))}
    val numReducers = 2;
    val listRDD = pairRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))
    val resultRDD =listRDD.flatMap{
      case (labe1,list) => {

        list.map((labe1, _))
      }
    }
    resultRDD.saveAsTextFile("output/")
  }
}
