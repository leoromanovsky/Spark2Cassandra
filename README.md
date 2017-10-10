# Spark2Cassandra

Spark Library for Bulk Loading into Cassandra

[![Build Status](https://travis-ci.org/leoromanovsky/Spark2Cassandra.svg?branch=master)](https://travis-ci.org/jparkie/Spark2Cassandra)

## Requirements

Spark2Cassandra supports Spark 2.2 and above.

| Spark2Cassandra Version | Cassandra Version |
| ------------------------| ----------------- |
| `3.0.0`                 | `2.1.5+`          |

## Downloads

#### SBT

```scala
libraryDependencies += "com.github.jparkie" %% "spark2cassandra" % "3.0.0-SNAPSHOT"
```

Add the following resolver if needed:

```scala
resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
```

#### Maven

```xml
libraryDependencies += "com.github.jparkie" %% "spark2cassandra" % "x.y.z"
```

It is planned for Spark2Cassandra to be available on the following:
- http://spark-packages.org/

## Features
- Utilizes Cassandra Java classes with https://github.com/datastax/spark-cassandra-connector to serialize `RDD`s or `DataFrame`s to SSTables.
- Streams SSTables to Cassandra nodes.

## Usage

### Bulk Loading into Cassandra

```scala
// Import the following to have access to the `bulkLoadToCassandra()` function for RDDs or DataFrames.
import com.github.jparkie.spark.cassandra.rdd._
import com.github.jparkie.spark.cassandra.sql._

val sparkConf = new SparkConf()
val sc = SparkContext.getOrCreate(sparkConf)
val sqlContext = SQLContext.getOrCreate(sc)

val rdd = sc.parallelize(???)

val df = sqlContext.read.parquet("<PATH>")

// Specify the `keyspaceName` and the `tableName` to write.
rdd.bulkLoadToCassandra(
  keyspaceName = "twitter",
  tableName = "tweets_by_date"
)

// Specify the `keyspaceName` and the `tableName` to write.
df.bulkLoadToCassandra(
  keyspaceName = "twitter",
  tableName = "tweets_by_author"
)
```

* [SparkCassRDDFunction.scala](https://github.com/jparkie/Spark2Cassandra/blob/master/src/main/scala/com/github/jparkie/spark/cassandra/rdd/SparkCassRDDFunctions.scala)
* [SparkCassDataFrameFunctions.scala](https://github.com/jparkie/Spark2Cassandra/blob/master/src/main/scala/com/github/jparkie/spark/cassandra/sql/SparkCassDataFrameFunctions.scala)

## Configurations

As Spark2Cassandra utilizes https://github.com/datastax/spark-cassandra-connector for serializations from Spark and session management, please refer to the following for more configurations: https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md.

### SparkCassWriteConf

For more, refer to: [SparkCassWriteConf.scala](https://github.com/jparkie/Spark2Cassandra/blob/master/src/main/scala/com/github/jparkie/spark/cassandra/conf/SparkCassWriteConf.scala)

| Property Name                                       | Default                                     | Description |
| --------------------------------------------------- |:-------------------------------------------:| ------------|
| `spark.cassandra.bulk.write.partitioner`            | org.apache.cassandra.dht.Murmur3Partitioner | The 'partitioner' defined in cassandra.yaml. |
| `spark.cassandra.bulk.write.throughput_mb_per_sec`  | Int.MaxValue                                | The maximum throughput to throttle. |
| `spark.cassandra.bulk.write.connection_per_host`    | 1                                           | The number of connections per host to utilize when streaming SSTables. |

### SparkCassServerConf

Refer to for more: [SparkCassServerConf.scala](https://github.com/jparkie/Spark2Cassandra/blob/master/src/main/scala/com/github/jparkie/spark/cassandra/conf/SparkCassServerConf.scala)

| Property Name                                      | Default                                                   | Description |
| -------------------------------------------------- |:---------------------------------------------------------:| ------------|
| `spark.cassandra.bulk.server.storage.port`         | 7000                                                      | The 'storage_port' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.sslStorage.port`      | 7001                                                      | The 'ssl_storage_port' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.internode.encryption` | "none"                                                    | The 'server_encryption_options:internode_encryption' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.keyStore.path`        | conf/.keystore                                            | The 'server_encryption_options:keystore' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.keyStore.password`    | cassandra                                                 | The 'server_encryption_options:keystore_password' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.trustStore.path`      | conf/.truststore                                          | The 'server_encryption_options:truststore' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.trustStore.password`  | cassandra                                                 | The 'server_encryption_options:truststore_password' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.protocol`             | TLS                                                       | The 'server_encryption_options:protocol' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.algorithm`            | SunX509                                                   | The 'server_encryption_options:algorithm' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.store.type`           | JKS                                                       | The 'server_encryption_options:store_type' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.cipherSuites`         | TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA | The 'server_encryption_options:cipher_suites' defined in cassandra.yaml. |
| `spark.cassandra.bulk.server.requireClientAuth`    | false                                                     | The 'server_encryption_options:require_client_auth' defined in cassandra.yaml. |

## Documentation

Scaladocs are currently unavailable.
