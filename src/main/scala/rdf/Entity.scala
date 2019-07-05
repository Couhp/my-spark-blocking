package rdf

import rdf.Util.{isNTripleFormat, getNtripleObject, isLinkEntity, getEntityFromUrl}

class Entity(val rdf: String = "") {

  var isAvailable = false
  var attribute, value = ""

  if (isNTripleFormat(rdf)) {
    isAvailable = true

    val rdfEntity: List[String] = getNtripleObject(rdf)
    attribute = rdfEntity(0)
    value = rdfEntity(1)
  }

  if (isLinkEntity(attribute)) {
    attribute = getEntityFromUrl(attribute)
  }

  if (isLinkEntity(value)) {
    value = getEntityFromUrl(value)
  }
}
