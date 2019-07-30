package attributeclustering

import config.SparkConfig.spark
import config.Environment.outputPath

object ParquetReader {
  def main(args: Array[String]): Unit = {
    val parquetFileDF = spark.read.parquet(outputPath)
    parquetFileDF.show(100)
  }
}
