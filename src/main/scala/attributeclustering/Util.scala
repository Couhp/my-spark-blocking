package attributeclustering

import org.apache.spark.rdd.RDD

object Util {

  def printrdd(rdd: RDD[Unit]): Unit ={
    rdd.collect.foreach(println)
  }

  private def getIndex(clusters: List[Set[String]], element1: String, element2: String): List[Int] = {

    val list_index: List[Int] =  clusters.indices.toList
                                                 .filter(i => (clusters(i) contains element1) ||
                                                              (clusters(i) contains element2))
    if (list_index.nonEmpty) return list_index
    // return -1 when it is not exist in all Cluster
    List(-1)
  }

  def clusterListMapping(maximumAttributeMatch: List[(String, String)]): List[Set[String]] ={
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

  def clusterMapping(maximumAttributeMatch: List[(String, String)]): Map[String, String] = {
    val clusters = clusterListMapping(maximumAttributeMatch)

    var result: Map[String, String] = Map()
    for ((cluster, index) <- clusters.view.zipWithIndex) {
      for(element <- cluster) {
        result += (element -> index.toString)
      }
    }
    result
  }

  def clusterMappingByEntity(maximumAttributeMatch: List[(String, String)]): Map[String, String] = {
    val clusters = clusterListMapping(maximumAttributeMatch)

    var result: Map[String, String] = Map()
    for ((cluster, index) <- clusters.view.zipWithIndex) {
      for(element <- cluster) {
        val oldVal = result getOrElse (index.toString, "")
        val newVal = if (oldVal != "") element + "," + oldVal else
                                       element
        result += (index.toString -> newVal)
      }
    }
    result
  }



  def main(args: Array[String]): Unit = {
    val example = List(("1a", "1b"),
                       ("1b", "1c"),
                       ("1x", "1f"),
                       ("1g", "1h"),
                       ("1g", "1k"),
                       ("1b", "1d"),
                       ("1x", "1m"))

    val result = clusterMapping(example)
    result.foreach(println)
  }
}
