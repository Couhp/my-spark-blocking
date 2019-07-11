package test

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("attribute-creation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val inputVal = Array(Array(1, "a"), Array(2, "bcd"), Array(3, "abcc"), Array(3, "ef"), Array(4, "ecf"))
    val input =  sc.parallelize(inputVal, 4)
    val rdd = input.map(x => (x(0), x(1))).reduceByKey{(x,y) => x+ " " +y}

    def f(key: Any, value: Any): TraversableOnce[Any] = {
      return List((key, value),(key, value))
    }

    rdd.flatMap {case (key, value) => {
      val ctx = TaskContext.get
      val stageId = ctx.stageId
      val partId = ctx.partitionId
      println(stageId, partId)
      f(key, value)}
    }.collect.foreach(println)
  }


}


