package attributeclustering

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import rdf.RDFReader.read
import config.SparkConfig

import config.Environment.{rdfDataset1, rdfDataset2}


object AttributeCreation {
  val sc = SparkConfig.spark
  var concatenationEntities: RDD[(String, String)] = sc.emptyRDD

  private def getTrigram(text: String): Set[String] = {
    text.sliding(3).map(p => p.toList.mkString("")).toList.toSet
  }

  def run(): RDD[(String, Set[String])] = {
    // Load input data from RDF local
    val dataset1 =  sc.parallelize(read(rdfDataset1).slice(1,5000))
    val dataset2 =  sc.parallelize(read(rdfDataset2).slice(5000, 12000))
    /** Transform into word and count.
      * - Map: (key)predicate attribute: (value)object
      * - Reduce: concatnation value by key, get trigram
      */
    val dataset1Entity: RDD[(String, String)] = dataset1.map(entity => ("0" + entity.head.toString, entity(1)))

    val dataset2Entity: RDD[(String, String)] = dataset2.map(entity => ("1" + entity.head.toString, entity(1)))

    concatenationEntities = dataset1Entity ++ dataset2Entity
    val trigramEntities = concatenationEntities.reduceByKey{(x, y) => x + " " + y}
                                               .mapValues(value => getTrigram(value))

    trigramEntities
  }

  def main(args: Array[String]): Unit = {
    val result = run()
    println(result.collect().deep.mkString("\n"))

  }
}
