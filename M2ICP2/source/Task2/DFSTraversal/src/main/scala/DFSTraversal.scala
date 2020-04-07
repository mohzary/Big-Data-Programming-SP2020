

object DFSTraversal{

  def main(args: Array[String]): Unit = {

    //to define variables V for nodes, and graph of type  map to store a list key value pairs where key is the node and
    // value is list of neighbors
    type V = Int
    type graph = Map[V, List[V]]

    // my usecase graph info
    val graphInfo : graph = Map(1 -> List(2, 3, 5), 2 -> List(), 3 ->List(2,4), 4 -> List(7), 5 -> List(3,7), 6 -> List(2), 7 -> List (6))

    //To create a function to compute DFS, takes node V as the start node and graph nodes and edges
    def DepthFirstSearch(start: V, graphInfo:graph): List[V] = {

      //to call dfs() on all vertices
      def DepthFirstSearch_0(v:V, visited: List[V]):List[V] = {

        //to check if the node is already visited, if it is not visited we take all neighbors that are not visited
        if (visited.contains(v))
          visited
        else{
          val adjacent_Vs: List[V] = graphInfo(v) filterNot visited.contains
          adjacent_Vs.foldLeft(v :: visited)((b,a) => DepthFirstSearch_0(a,b))
        }
      }
      DepthFirstSearch_0(start, List()).reverse
    }
    // To call DFS function and print result
    val DFSResult = DepthFirstSearch(1, graphInfo)
    println("DFS Result For This Graph IS:")
    println(DFSResult.mkString(","))

  }
}