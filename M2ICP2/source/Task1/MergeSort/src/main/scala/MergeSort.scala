import org.apache.spark.{SparkConf, SparkContext}

object MergeSort {

  def main(args: Array[String]): Unit = {


    //To define new spark configuration
    val conf = new SparkConf().
      setMaster("local").
      setAppName("MergeSort")

    // To create spark context sc
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // The input data as list of random number in random order
    val inputData = List(List(38,27,43,3,9,82,10))

    // To create RDD from input data using parallelize method
    val RDD = sc.parallelize(inputData)

    // The merge sort function
    def mergeSort(listx: List[Int]): List[Int] = {

      //To compute the middle of the list so we can divide the list into two parts
      val mid = listx.length / 2
      if (mid == 0 ) listx
      else{
         def merge(listx: List[Int], listy: List[Int]): List[Int] =
           (listx, listy) match{
             case(Nil, listy) => listy
             case(listx, Nil) => listx
             case(x :: listx1, y :: listy1) =>
               if(x < y) x::merge(listx1, listy)
               else y :: merge(listx, listy1)
           }
        val (left, right) = listx splitAt(mid)
        merge(mergeSort(left), mergeSort(right))
      }

    }

    // to call the merge sort function and store the output in the result variable
    val result = RDD.map(mergeSort)

    // To show the final result in a list format
    print("Final Result of Merge Sort Algorithm: ")
    print(result.collect().toList)



  }
}