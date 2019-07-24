package attributeclustering

import Util.clusterMapping
import org.apache.spark.rdd.RDD

object AttributeBlocking {
  def run(): Unit = {
    val maximumAttributeMatch: RDD[(String,  String)] = BestMatch.run()
    val clusters = clusterMapping(maximumAttributeMatch.collect().toList)
    clusters.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    run()
  }
}
