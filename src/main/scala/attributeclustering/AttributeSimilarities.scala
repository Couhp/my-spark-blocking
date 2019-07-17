package attributeclustering

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

object AttributeSimilarities {
  def getMapperJoinById(currentId: Int, totalMapper: Int, key: String, value: Set[String]): TraversableOnce[(String, (String, Set[String]))] = {
    println(currentId, totalMapper)
    // Convert to (mapperID, a copy of the input)
    val result = (0 until totalMapper).map(id => {
      if (id < currentId) {
        (id.toString + "_" + currentId.toString, (key, value))
      } else {
        (currentId.toString + "_" + id.toString, (key, value))
      }
    })

    result
  }

  def jaccardSimilarity(set1: Set[String], set2: Set[String]): Float = {

    val union = (set1 | set2).size
    val intersect = (set1 & set2).size
//    println(set1, set2)
//    println(union, intersect)
    if (union != 0) intersect.toFloat/union else 0
  }

  def run(): Unit = {
    /**
      * input = the output of AttributesMapper:
      * input key: datasourceIDpredicate (datasourceId is either 0 or 1)
      * input value: all the values of this predicate (taking data source into account)
      * output
      *
      * mapper
      * key: a pair determined by totalMappers and currentMapper (separated by underslash"_")
      * value: List(mapperID, a copy of the input) (both input key and input value separated by ";;;")
      *
      * reducer
      * key: datasourceIdOfPred1;;;predicate1
      * value: datasourceIdOfPred2;;;predicate2;;;similarity with predicate1
      *
      */
    val trigramEntities: RDD[(String, Set[String])] = AttributeCreation.run()

    val numberPartitions: Int = trigramEntities.partitions.length

    val valueByMapperId: RDD[(String, (String, Set[String]))] = trigramEntities
        .flatMap { case (key, value) => {
        val ctx = TaskContext.get
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        getMapperJoinById(partId, numberPartitions, key, value)
      }
    }
    /* Joined value together\
     * For per key now have:
     *   (key) - ((pre1, set1), (pre2, set2))
     */
    val joined = valueByMapperId.join(valueByMapperId).filter({case (_, (v1, v2)) => v1._1 < v2._1})
    joined.collect.foreach(println)
    val similarityValue = joined.map({
      case (key, (v1, v2)) => (v1._1, (v2._1, jaccardSimilarity(v1._2, v2._2)))
    }).collect().foreach(println)

  }

  def main(args: Array[String]): Unit = {
    run()
  }

}
