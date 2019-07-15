package config

import org.apache.spark.{SparkConf, SparkContext}

object SparkConfig {
  // Create a Scala Spark Context.
    private val conf = new SparkConf().setAppName("attribute-creation").setMaster("local[2]")
    val spark = new SparkContext(conf)

}
