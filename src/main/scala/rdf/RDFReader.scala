package rdf

import scala.io.Source

object RDFReader {

    def read(datasetPath: String): List[List[String]] = {
        val lines = Source.fromFile(datasetPath).getLines.toList
        val data: List[String] = lines.slice(1, 240)

        val entityCollection = data.map(e => new Entity(e))
          .filter(_.isAvailable)
          .map(e => List(e.attribute, e.value))

        entityCollection
    }

    def main(args: Array[String]): Unit = {
        read("data/locah/locahNewEntityIds.nt")//.deep.mkString("\n"))
    }

}
