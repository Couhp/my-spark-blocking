package rdf

import scala.io.Source
import config.Environment.rdfFilePath

object RDFReader {
  def main(args: Array[String]): Unit = {

    val lines = Source.fromFile(rdfFilePath).getLines.toList
    var data: String = lines(1)
    var entity = new Entity(data)
    println(entity)
  }
}
