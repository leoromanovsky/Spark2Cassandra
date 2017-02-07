package com.github.jparkie.spark.cassandra

import org.apache.spark.sql.{ Dataset, Row }

package object sql {

  import scala.language.implicitConversions

  /**
   * Implicitly lift a [[Dataset[Row]]] with [[SparkCassDataFrameFunctions]].
   *
   * @param dataFrame A [[Dataset[Row]]] to lift.
   * @return Enriched [[Dataset[Row]]] with [[SparkCassDataFrameFunctions]].
   */
  implicit def sparkCassDataFrameFunctions(dataFrame: Dataset[Row]): SparkCassDataFrameFunctions = {
    new SparkCassDataFrameFunctions(dataFrame)
  }
}
