package attributeclustering

import Util.{clusterMapping, clusterMappingByEntity}
import config.Environment.outputPath
import config.SparkConfig.spark
import org.apache.spark.rdd.RDD


object AttributeBlocking {

  def getTokensBlock(predicates: List[(String, String)], clusters: Map[String, String]): Set[String] = {
    var blocking: Set[String] = Set()
    for(predicate: (String, String) <- predicates) {
      val cluster = clusters(predicate._1)
      val tokens = predicate._2.split(" ")
      for(token <- tokens) {
        blocking = blocking ++ Set(cluster + "_" + token)
      }
    }

    blocking
  }

  def getKeepingEntity(entities: Map[String, String]): Set[String] = {
    var keeping: Set[String] = Set()
    var checkedCluster: Set[String] = Set()

    for((k,v) <- entities) {
      if (!checkedCluster(v)) {
        keeping += k
        checkedCluster += v
      }
    }
    keeping
  }

  def compareSim(entity1: (String, Set[String]), entity2: (String, Set[String])): ((String, String), Float) ={
    val set1 = entity1._2
    val set2 = entity2._2
    val union = (set1 | set2).size
    val intersect = (set1 & set2).size
    val sim = if (union != 0) intersect.toFloat/union else 0

    ((entity1._1, entity2._1), sim)
  }

  def run(): Unit = {
    val maximumAttributeMatch: RDD[(String, String)] = BestMatch.run()

    val clusters: Map[String, String] = clusterMapping(maximumAttributeMatch.collect().toList)

    val concatenationEntities = AttributeCreation.concatenationEntities.groupBy(_._1)
                                                                       .map(p => (p._1,p._2.map(_._2)))

    val token_blocking: RDD[(Int, (String, Set[String]))] = concatenationEntities.map{case(key, value) =>
      (key, getTokensBlock(value.toList, clusters))
    }.map(p => (0, p))

    val duplicates = token_blocking.join(token_blocking).filter{case (_, (x, y)) => x._1 < y._1}
                                       .map({case(k, v) => {
                                         val sim = compareSim(v._1, v._2)
                                         (sim._1, sim._2)
                                       }})
                                       .filter(_._2 > 0.5)
                                       .map({case(k,v) => (k._1, k._2)})
                                       .collect().toList

    val duplicateCluster = clusterMapping(duplicates)
    val numberDuplicate = duplicateCluster.size  // use this number to know how many duplicate in dataset
    val keppingDuplicate: Set[String] = getKeepingEntity(duplicateCluster)

    val newDataset = AttributeCreation.concatenationEntities.filter({case(k,v) => !(duplicateCluster contains k) ||
                                                                                    keppingDuplicate(k)
                                                                   })
                                                            .map({case(k,v) => (k, v._1, v._2)})

    val dfWithoutSchema = spark.createDataFrame(newDataset)
    dfWithoutSchema.write.parquet(outputPath)

  }

  def main(args: Array[String]): Unit = {
    run()
  }
}
