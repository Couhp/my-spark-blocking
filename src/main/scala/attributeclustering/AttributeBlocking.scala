package attributeclustering

import Util.clusterMapping
import config.Environment.outputPath
import org.apache.spark.rdd.RDD

object AttributeBlocking {

  def getTokensBlock(cluster: String, key: String, value: String): List[(String, String)] = {
    val tokens = value.split(" ")
    val result = tokens.map(token => ("Collection_" + cluster + "-" + "token_" + token,
                                       key + ":" + value))

    result.toList
  }

  def run(): Unit = {
    val maximumAttributeMatch: RDD[(String, String)] = BestMatch.run()
    val clusters = clusterMapping(maximumAttributeMatch.collect().toList)

    val concatenationEntities = AttributeCreation.concatenationEntities

    val token_blocking: RDD[(String, String)] = concatenationEntities.flatMap{case(key, value) => {
      getTokensBlock(clusters(key), key, value)
    }}.reduceByKey{case(v1, v2) => v1 + "\n" + v2}

    token_blocking.saveAsTextFile(outputPath)
  }

  def main(args: Array[String]): Unit = {
    run()
  }
}
