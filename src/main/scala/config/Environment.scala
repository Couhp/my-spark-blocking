package config

import com.typesafe.config.ConfigFactory

object Environment {

  val rdfDataset1 = ConfigFactory.load().getString("dataset1")
  val rdfDataset2 = ConfigFactory.load().getString("dataset2")
  val sparkMaster = ConfigFactory.load().getString("sparkMaster")
  val outputPath = ConfigFactory.load().getString("outputPath")
}
