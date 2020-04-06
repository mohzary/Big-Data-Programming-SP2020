

object DFSTraversal{
  def main(args: Array[String]): Unit = {
    type V = Int
    type graph = Map[V, List[V]]

    val graphInfo : graph = Map(1 -> List(2, 3, 5), 2 -> List(), 3 ->List(2,4), 4 -> List(7), 5 -> List(3,7), 6 -> List(2), 7 -> List (6))

    def DepthFirstSearch(start: V, graphInfo:graph): List[V] = {
      def DepthFirstSearch_0(v:V, visited: List[V]):List[V] = {
        if (visited.contains(v))
          visited
        else{
          val adjacent_Vs: List[V] = graphInfo(v) filterNot visited.contains
          adjacent_Vs.foldLeft(v :: visited)((b,a) => DepthFirstSearch_0(a,b))
        }
      }
      DepthFirstSearch_0(start, List()).reverse
    }

    val DFSResult = DepthFirstSearch(1, graphInfo)
    println("DFS Result For This Graph IS:")
    println(DFSResult.mkString(","))

  }
}