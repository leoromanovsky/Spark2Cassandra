package com.github.jparkie.spark.cassandra.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{ MustMatchers, WordSpec }

class SparkCassDataFrameFunctionsSpec extends WordSpec with MustMatchers {
  def conf: SparkConf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")
      .set("spark.sql.shuffle.partitions", "1")
  val sc = SparkSession.builder().config(conf).getOrCreate().sparkContext
  "Package com.github.jparkie.spark.cassandra.sql" must {
    "lift DataFrame into SparkCassDataFrameFunctions" in {
      val sparkContext = SparkSession.builder().getOrCreate()

      import sparkContext.implicits._

      val testRDD = sc.parallelize(1 to 25)
        .map(currentNumber => (currentNumber.toLong, s"Hello World: $currentNumber!"))
      val testDataFrame = testRDD.toDF("test_key", "test_value")

      // If internalSparkContext is available, RDD was lifted.
      testDataFrame.internalSparkContext
    }
  }
}
