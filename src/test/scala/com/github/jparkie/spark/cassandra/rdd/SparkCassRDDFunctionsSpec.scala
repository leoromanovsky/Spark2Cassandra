package com.github.jparkie.spark.cassandra.rdd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{ MustMatchers, WordSpec }

class SparkCassRDDFunctionsSpec extends WordSpec with MustMatchers {
  def conf: SparkConf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")
      .set("spark.sql.shuffle.partitions", "1")
  val sc = SparkSession.builder().config(conf).getOrCreate().sparkContext
  "Package com.github.jparkie.spark.cassandra.rdd" must {
    "lift RDD into SparkCassRDDFunctions" in {
      val testRDD = sc.parallelize(1 to 25)
        .map(currentNumber => (currentNumber.toLong, s"Hello World: $currentNumber!"))

      // If internalSparkContext is available, RDD was lifted.
      testRDD.internalSparkContext
    }
  }
}
