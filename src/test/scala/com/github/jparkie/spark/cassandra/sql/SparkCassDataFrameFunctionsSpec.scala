package com.github.jparkie.spark.cassandra.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{MustMatchers, Outcome, WordSpec}

class SparkCassDataFrameFunctionsSpec extends WordSpec with MustMatchers with DataFrameSuiteBase {
  "Package com.github.jparkie.spark.cassandra.sql" must {
    "lift DataFrame into SparkCassDataFrameFunctions" in {
      import spark.implicits._

      val testRDD = sc.parallelize(1 to 25)
        .map(currentNumber => (currentNumber.toLong, s"Hello World: $currentNumber!"))
      val testDataFrame = testRDD.toDF("test_key", "test_value")

      // If internalSparkContext is available, RDD was lifted.
      testDataFrame.internalSparkContext
    }
  }
}
