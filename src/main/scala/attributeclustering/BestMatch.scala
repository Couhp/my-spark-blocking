package attributeclustering

import org.apache.spark.rdd.RDD

object BestMatch {

  def run(): Unit = {
    val similarityValues: RDD[(String, (String, Float))] = AttributeSimilarities.run()

    val inversedSimilarityValues = similarityValues.flatMap({
      case(key, (another_key, similarity)) => List((another_key, (key, similarity)),
                                                   (key, (another_key, similarity)))
    })
//    inversedSimilarityValues.collect().foreach(println)
    val MinimumSimThreshold = 0.0
    val maximum = inversedSimilarityValues.reduceByKey({case(v1,v2) =>
      if (v1._2 > v2._2) {v1}
      else v2
    }).filter({case(k, v) => v._2 > MinimumSimThreshold})

    maximum.collect().foreach(println)

  }

  def main(args: Array[String]): Unit = {
    run()
  }
}


