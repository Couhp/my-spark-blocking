package attributecreation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import config.Environment.rdfDataset1
import rdf.RDFReader.read

object AttributeCreation {
  private def getTrigram(text: String): Set[String] = {
    text.sliding(3).map(p => p.toList.mkString("")).toList.toSet
  }

  def mapReduce(): RDD[(String, Set[String])] = {
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("attribute-creation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // Load input data from RDF local
    val input =  sc.parallelize(read(rdfDataset1))
    // Transform into word and count.
    // - Map: (key)predicate attribute: (value)object
    // - Reduce: concatnation value by key, add trigram
    val concatenationEntities: RDD[(String, String)] = input.map(entity => ("1" + entity(0).toString, entity(1)))
                                     .reduceByKey{(x, y) => x + " " + y}

    val trigramEntities = concatenationEntities.mapValues(value => getTrigram(value))
    return trigramEntities
  }

  def main(args: Array[String]): Unit = {
    val result = mapReduce()
    println(result.collect().deep.mkString("\n"))

  }
}
