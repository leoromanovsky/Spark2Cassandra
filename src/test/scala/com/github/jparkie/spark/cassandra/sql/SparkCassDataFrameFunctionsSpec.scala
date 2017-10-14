package com.github.jparkie.spark.cassandra.sql

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{MustMatchers, WordSpec}

class SparkCassDataFrameFunctionsSpec extends WordSpec with MustMatchers with SharedSparkContext {
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
