package com.github.jparkie.spark.cassandra

import com.typesafe.scalalogging.LazyLogging
import org.apache.cassandra.utils.OutputHandler
import org.slf4j.Logger

/**
 * A wrapper for [[Logger]] for [[com.github.jparkie.spark.cassandra.client.SparkCassSSTableLoaderClient]].
 */
class SparkCassOutputHandler extends OutputHandler with LazyLogging {
  override def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  override def warn(msg: String, th: Throwable): Unit = {
    logger.warn(msg, th)
  }

  override def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  override def output(msg: String): Unit = {
    logger.info(msg)
  }
}
