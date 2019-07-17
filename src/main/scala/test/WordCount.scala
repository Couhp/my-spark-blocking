package test

import config.Environment.rdfDataset1
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import rdf.RDFReader.read

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("attribute-creation").setMaster("local[2]")
    val sc = new SparkContext(conf)

//    val inputVal = Array(Array(1, "a"), Array(2, "bcd"))
    val input =  sc.parallelize(read(rdfDataset1))
    val rdd = input.map(x => (x(0), x(1))).reduceByKey{(x,y) => x+ " " +y}

    def f(key: Any, value: Any): TraversableOnce[Any] = {
      return List((key, value),(key, value))
    }

    rdd.flatMap {case (key, value) => {
      val ctx = TaskContext.get
      val stageId = ctx.stageId
      val partId = ctx.partitionId.toString
      println(stageId, partId)
      List((partId, value))}
    }.collect.foreach(println)
  }


}


