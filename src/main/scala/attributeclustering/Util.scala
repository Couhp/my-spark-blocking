package attributeclustering

object Util {

  private def getIndex(clusters: List[Set[String]], element1: String, element2: String): List[Int] = {

    val list_index: List[Int] =  clusters.indices.toList
                                                 .filter(i => (clusters(i) contains element1) ||
                                                              (clusters(i) contains element2))
    if (list_index.nonEmpty) return list_index
    // return -1 when it is not exist in all Cluster
    List(-1)
  }

  def clusterMapping(maximumAttributeMatch: List[(String, String)]): List[Set[String]] = {
    // Get clusters for all atribute match pairs
    var clusters: List[Set[String]] = List()

    for(pairs <- maximumAttributeMatch) {
      val element0 = pairs._1
      val element1 = pairs._2

      val clusterIndexs = getIndex(clusters, element0, element1)

      if(clusterIndexs.head == -1) {
        clusters = clusters :+ Set(element0, element1)
      }
      else if(clusterIndexs.size > 1){
        val newMergedCluster = clusters(clusterIndexs.head) union
                               clusters(clusterIndexs(1))
        clusters = clusters.indices.filter(!clusterIndexs.contains(_))
                                       .map(i => clusters(i)).toList
        clusters = clusters :+ newMergedCluster
      }
      else {
        val clusterIndex = clusterIndexs.head
        clusters = clusters.updated(clusterIndex, clusters(clusterIndex) + element0)
        clusters = clusters.updated(clusterIndex, clusters(clusterIndex) + element1)
      }
    }

    clusters
  }

  def main(args: Array[String]): Unit = {
    val example = List(("a", "b"),
                       ("b", "c"),
                       ("x", "f"),
                       ("g", "h"),
                       ("g", "k"),
                       ("b", "d"),
                       ("x", "m"))

    val result: List[Set[String]] = clusterMapping(example)
    result.foreach(println)
  }
}
