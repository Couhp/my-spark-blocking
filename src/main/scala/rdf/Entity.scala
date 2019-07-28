package rdf

import rdf.Util.{isNTripleFormat, getNtripleObject, isLinkEntity, getEntityFromUrl}

class Entity(val rdf: String = "") {

  var isAvailable = false
  var entity, attribute, value = ""

  if (isNTripleFormat(rdf)) {
    isAvailable = true

    val rdfEntity: List[String] = getNtripleObject(rdf)
    entity = rdfEntity(0)
    attribute = rdfEntity(1)
    value = rdfEntity(2)
  }

  if (isLinkEntity(attribute)) {
    attribute = getEntityFromUrl(attribute)
  }

  if (isLinkEntity(value)) {
    value = getEntityFromUrl(value)
  }
}
