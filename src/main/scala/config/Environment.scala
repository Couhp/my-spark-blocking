package config

import com.typesafe.config.ConfigFactory

object Environment {

  val rdfFilePath = ConfigFactory.load().getString("rdfFilePath")
  val sparkMaster = ConfigFactory.load().getString("sparkMaster")
}
