package attributeclustering

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

object AttributeSimilarities {
  def getMapperJoinById(currentId: Int, totalMapper: Int, value: Any): TraversableOnce[Any] = {
    // Convert to (mapperID, a copy of the input)
    val result = (0 until totalMapper).map(id => {
      if (id < currentId) {
        (id.toString + "_" + totalMapper.toString, value)
      } else {
        (totalMapper.toString + "_" + id.toString, value)
      }
    })

    result
  }

  def run(): Unit = {
    /**
      * input = the output of AttributesMapper:
      * 	input key: datasourceIDpredicate (datasourceId is either 0 or 1)
      * 	input value: all the values of this predicate (taking data source into account)
      * output
      * 	key: a pair determined by totalMappers and currentMapper (separated by underslash"_")
      * 	value: List(mapperID, a copy of the input) (both input key and input value separated by ";;;")
      */
    val trigramEntities: RDD[(String, Set[String])] = AttributeCreation.run()

    val numberPartitions: Int = trigramEntities.partitions.length

    val valueByMapperId = trigramEntities.flatMap {case (key, value) => {
      val ctx = TaskContext.get
      val stageId = ctx.stageId
      val partId = ctx.partitionId

      getMapperJoinById(partId, numberPartitions, value)}
    }



  }

  def main(args: Array[String]): Unit = {
    run()
  }

}
