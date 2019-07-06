package rdf

import scala.io.Source
import config.Environment.rdfFilePath

object RDFReader {

    private val lines = Source.fromFile(rdfFilePath).getLines.toList
    private val data: List[String] = lines.slice(1, 240)

    val entityCollection = data.map(e => new Entity(e))
                              .filter(_.isAvailable)
                              .map(e => Array(e.attribute, e .value))

//    println(entityTypedData.deep.mkString("\n"))
}
