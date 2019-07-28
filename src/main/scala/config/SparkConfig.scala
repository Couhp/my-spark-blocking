package config

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkConfig {
  // Create a Scala Spark Context.
//    private val conf = new SparkConf().setAppName("attribute-creation").setMaster("local[2]")
//    val sparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder.master("local[2]").getOrCreate
    val sparkConf = spark.sparkContext
}
