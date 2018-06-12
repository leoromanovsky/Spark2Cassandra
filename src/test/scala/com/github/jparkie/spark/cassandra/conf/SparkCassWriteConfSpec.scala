package com.github.jparkie.spark.cassandra.conf

import com.datastax.spark.connector.writer.TTLOption
import org.apache.cassandra.dht.{ ByteOrderedPartitioner, Murmur3Partitioner, RandomPartitioner }
import org.apache.spark.SparkConf
import org.scalatest.{ MustMatchers, WordSpec }

class SparkCassWriteConfSpec extends WordSpec with MustMatchers {
  "SparkCassWriteConf" must {
    "be extracted from SparkConf successfully" in {
      val inputSparkConf = new SparkConf()
        .set("spark.cassandra_bulk.write.partitioner", "org.apache.cassandra.dht.ByteOrderedPartitioner")
        .set("spark.cassandra_bulk.write.throughput_mb_per_sec", "1")
        .set("spark.cassandra_bulk.write.connection_per_host", "2")
        .set("spark.cassandra_bulk.write.ttl", "3")

      val outputSparkCassWriteConf = SparkCassWriteConf.fromSparkConf(inputSparkConf)

      outputSparkCassWriteConf.partitioner mustEqual "org.apache.cassandra.dht.ByteOrderedPartitioner"
      outputSparkCassWriteConf.throughputMiBPS mustEqual 1
      outputSparkCassWriteConf.connectionsPerHost mustEqual 2
      outputSparkCassWriteConf.ttl mustEqual TTLOption.constant(3)
    }

    "set defaults when no properties set in SparkConf" in {
      val inputSparkConf = new SparkConf()

      val outputSparkCassWriteConf = SparkCassWriteConf.fromSparkConf(inputSparkConf)

      outputSparkCassWriteConf.partitioner mustEqual
        SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_PARTITIONER.default
      outputSparkCassWriteConf.throughputMiBPS mustEqual
        SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_THROUGHPUT_MB_PER_SEC.default
      outputSparkCassWriteConf.connectionsPerHost mustEqual
        SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_CONNECTIONS_PER_HOST.default
      outputSparkCassWriteConf.ttl mustEqual TTLOption.defaultValue
    }

    "be extracted from SparkConf successfully and use defaults if not set" in {
      val inputSparkConf = new SparkConf()
        .set("spark.cassandra_bulk.write.partitioner", "org.apache.cassandra.dht.ByteOrderedPartitioner")
        .set("spark.cassandra_bulk.write.throughput_mb_per_sec", "1")

      val outputSparkCassWriteConf = SparkCassWriteConf.fromSparkConf(inputSparkConf)

      outputSparkCassWriteConf.partitioner mustEqual "org.apache.cassandra.dht.ByteOrderedPartitioner"
      outputSparkCassWriteConf.throughputMiBPS mustEqual 1
      outputSparkCassWriteConf.connectionsPerHost mustEqual
        SparkCassWriteConf.SPARK_CASSANDRA_BULK_WRITE_CONNECTIONS_PER_HOST.default
      outputSparkCassWriteConf.ttl mustEqual TTLOption.defaultValue
    }

    "reject invalid partitioner in SparkConf" in {
      val inputSparkConf = new SparkConf()
        .set("spark.cassandra_bulk.write.partitioner", "N/A")

      intercept[IllegalArgumentException] {
        SparkCassWriteConf.fromSparkConf(inputSparkConf)
      }
    }

    "getIPartitioner() correctly per partitioner" in {
      val sparkCassWriteConf1 = SparkCassWriteConf("org.apache.cassandra.dht.Murmur3Partitioner")
      assert(sparkCassWriteConf1.getIPartitioner.isInstanceOf[Murmur3Partitioner])
      val sparkCassWriteConf2 = SparkCassWriteConf("org.apache.cassandra.dht.RandomPartitioner")
      assert(sparkCassWriteConf2.getIPartitioner.isInstanceOf[RandomPartitioner])
      val sparkCassWriteConf3 = SparkCassWriteConf("org.apache.cassandra.dht.ByteOrderedPartitioner")
      assert(sparkCassWriteConf3.getIPartitioner.isInstanceOf[ByteOrderedPartitioner])
    }
  }
}
