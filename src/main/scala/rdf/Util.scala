package rdf

object Util {
  def isNTripleFormat(stringInput: String): Boolean = {
    val nTripleSpliter = stringInput.split(" ")
    if (nTripleSpliter.size >= 3) return true
    false
  }

  def getNtripleObject(stringInput: String): List[String] = {
    val nTripleSpliter = stringInput.split(" ")
    return List(nTripleSpliter(1), nTripleSpliter.takeRight(nTripleSpliter.size - 2).mkString(" "))
  }

  def isLinkEntity(stringInput: String): Boolean = {
    if (stringInput contains "http") {
      return true
    }
    false
  }

  def getEntityFromUrl(stringInput: String): String = {
    val entity = stringInput.split("[/#:]").last

    if (stringInput(0) == '<') {
      return entity.slice(0, entity.size - 1)
    }
    entity
  }
}
