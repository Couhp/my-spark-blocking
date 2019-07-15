package attributeclustering

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import rdf.RDFReader.read
import config.SparkConfig

import config.Environment.rdfDataset1


object AttributeCreation {
  private def getTrigram(text: String): Set[String] = {
    text.sliding(3).map(p => p.toList.mkString("")).toList.toSet
  }

  def run(): RDD[(String, Set[String])] = {
    val sc = SparkConfig.spark
    // Load input data from RDF local
    val input =  sc.parallelize(read(rdfDataset1))
    /** Transform into word and count.
      * - Map: (key)predicate attribute: (value)object
      * - Reduce: concatnation value by key, get trigram
      */
    val concatenationEntities: RDD[(String, String)] = input.map(entity => ("1" + entity(0).toString, entity(1)))
                                     .reduceByKey{(x, y) => x + " " + y}

    val trigramEntities = concatenationEntities.mapValues(value => getTrigram(value))

    trigramEntities
  }

  def main(args: Array[String]): Unit = {
    val result = run()
    println(result.collect().deep.mkString("\n"))

  }
}
