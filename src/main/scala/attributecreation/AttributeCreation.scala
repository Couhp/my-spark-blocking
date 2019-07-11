package attributecreation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//import config.Environment.sparkMaster
import rdf.RDFReader.entityCollection

object AttributeCreation {
  def mapReduce(): RDD[(String, String)] = {
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("attribute-creation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // Load input data from RDF local
    val input =  sc.parallelize(entityCollection)
    // Transform into word and count.
    // - Map: (key)predicate attribute: (value)object
    // - Reduce: concatnation value by key
    val concatenationEntities = input.map(entity => (entity(0), entity(1)))
                                     .reduceByKey{(x, y) => x + " " + y}

    return concatenationEntities
  }

  def main(args: Array[String]): Unit = {
    val result = mapReduce()
    println(result.collect().deep.mkString("\n"))

  }
}
