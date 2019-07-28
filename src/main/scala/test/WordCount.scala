package test

import config.Environment.rdfDataset1
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import rdf.RDFReader.read
import config.SparkConfig.spark

object WordCount {
  def get(a: String, b: String): (String, String) = {
    return (a, b)
  }
  def main(args: Array[String]) {

    val x = spark.read.parquet("data/ouput/result.parquet")
    x.show()


  }


}


