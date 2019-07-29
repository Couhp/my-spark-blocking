package test

import config.Environment.rdfDataset1
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import rdf.RDFReader.read
import config.SparkConfig.sparkConf

object WordCount {
  def get(a: String, b: String): (String, String) = {
    return (a, b)
  }
  def main(args: Array[String]) {

    val linesRdd = sparkConf.parallelize(List("qwqew12", "fsds", "sgse23"))
    val lineswithnum=linesRdd.filter(line => line.exists(_.isDigit)).count()


  }


}


