package com.github.jparkie.spark.cassandra.client

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{ Row, SimpleStatement }
import com.datastax.oss.driver.api.core.metadata.token.{ TokenRange => DriverTokenRange }
import com.github.jparkie.spark.cassandra.conf.SparkCassServerConf
import org.apache.cassandra.config.{ CFMetaData, ColumnDefinition }
import org.apache.cassandra.cql3.ColumnIdentifier
import org.apache.cassandra.db.marshal.ReversedType
import org.apache.cassandra.dht.{ IPartitioner, Token }
import com.datastax.oss.driver.internal.core.metadata.token.{ ByteOrderedToken, Murmur3Token, RandomToken }
import com.datastax.oss.driver.api.core.metadata.token.{ Token => IToken }
import org.apache.cassandra.io.sstable.SSTableLoader
import org.apache.cassandra.schema.{ CQLTypeParser, SchemaKeyspace, Types }
import org.apache.cassandra.streaming.StreamConnectionFactory
import org.apache.cassandra.tools.BulkLoadConnectionFactory
import org.apache.cassandra.utils.FBUtilities
import com.datastax.oss.driver.internal.core.util.Strings

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Coordinates the streaming of SSTables to the proper Cassandra data nodes through automated column family discovery.
 *
 * Warning: This borrows heavily from [[org.apache.cassandra.utils.NativeSSTableLoaderClient]]
 *
 * @param session             A prolonged [[com.datastax.oss.driver.api.core.session.Session]] to query system keyspaces.
 * @param sparkCassServerConf Configurations to connect to Cassandra Transport Layer.
 */
class SparkCassSSTableLoaderClient(
  val session:             CqlSession,
  val sparkCassServerConf: SparkCassServerConf
) extends SSTableLoader.Client {
  import SparkCassSSTableLoaderClient._

  private[client] val tables: mutable.Map[String, CFMetaData] = mutable.HashMap.empty[String, CFMetaData]

  override def init(keyspace: String): Unit = {
    val tokenMap = session.getMetadata.getTokenMap.get

    val partitioner = FBUtilities.newPartitioner(tokenMap.getPartitionerName)

    val tokenRanges: mutable.Set[DriverTokenRange] = tokenMap.getTokenRanges.asScala
    val tokenFactory: Token.TokenFactory = partitioner.getTokenFactory

    for (tokenRange <- tokenRanges) {
      val endpoints = tokenMap.getReplicas(Strings.doubleQuote(keyspace), tokenRange).asScala

      val range = new TokenRange(
        tokenFactory.fromString(getValue(tokenRange.getStart)),
        tokenFactory.fromString(getValue(tokenRange.getEnd))
      )

      for (endpoint <- endpoints) addRangeForEndpoint(range, endpoint.getBroadcastRpcAddress.get.getAddress)
    }

    val types = fetchTypes(keyspace, session)

    fetchTables(keyspace, session, partitioner, types)
    // We only need the CFMetaData for the views, so we only load that.
    fetchViews(keyspace, session, partitioner, types)
  }

  override def stop(): Unit = {
    if (!session.isClosed) {
      session.close()
    }
  }

  override def getTableMetadata(tableName: String): CFMetaData = {
    tables(tableName)
  }

  override def getConnectionFactory: StreamConnectionFactory = {
    new BulkLoadConnectionFactory(
      sparkCassServerConf.storagePort,
      sparkCassServerConf.sslStoragePort,
      sparkCassServerConf.getServerEncryptionOptions,
      false
    )
  }

  // Only 3 tokens implement Token, cf: https://github.com/datastax/java-driver/tree/4.x/core/src/main/java/com/datastax/oss/driver/internal/core/metadata/token
  private def getValue(token: IToken): String = token match {
    case token: Murmur3Token     => token.getValue.toString
    case token: RandomToken      => token.getValue.toString
    case token: ByteOrderedToken => token.getValue.toString
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.fetchTypes
  private def fetchTypes(keyspace: String, session: CqlSession): Types = {
    val query = SimpleStatement
      .builder(s"SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.TYPES} WHERE keyspace_name = ?")
      .addPositionalValues(keyspace)
      .build()
    val types = Types.rawBuilder(keyspace)
    import scala.collection.JavaConversions._
    for (row <- session.execute(query)) {
      val name = row.getString("type_name")
      val fieldNames = row.getList("field_names", classOf[String])
      val fieldTypes = row.getList("field_types", classOf[String])
      types.add(name, fieldNames, fieldTypes)
    }
    types.build
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.fetchTables
  private def fetchTables(keyspace: String, session: CqlSession, partitioner: IPartitioner, types: Types): Unit = {
    val columnName = "table_name"
    val query = SimpleStatement
      .builder(s"SELECT $columnName FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.TABLES} WHERE keyspace_name = ?")
      .addPositionalValues(keyspace)
      .build()
    import scala.collection.JavaConversions._
    for (row <- session.execute(query)) {
      val name = row.getString(columnName)
      tables.put(name, createTableMetadata(keyspace, session, partitioner, false, row, name, types))
    }
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.fetchViews
  private def fetchViews(keyspace: String, session: CqlSession, partitioner: IPartitioner, types: Types): Unit = {
    val columnName = "view_name"
    val query = SimpleStatement
      .builder(s"SELECT $columnName FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.VIEWS} WHERE keyspace_name = ?")
      .addPositionalValues(keyspace)
      .build()
    import scala.collection.JavaConversions._
    for (row <- session.execute(query)) {
      val name = row.getString(columnName)
      tables.put(name, createTableMetadata(keyspace, session, partitioner, true, row, name, types))
    }
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.createTableMetadata
  private def createTableMetadata(keyspace: String, session: CqlSession, partitioner: IPartitioner, isView: Boolean, row: Row, name: String, types: Types): CFMetaData = {
    val id = row.getUuid("id")

    val flags =
      if (isView) Set.empty[CFMetaData.Flag]
      else CFMetaData.flagsFromStrings(row.getSet("flags", classOf[String])).asScala

    val isSuper = flags.contains(CFMetaData.Flag.SUPER)
    val isCounter = flags.contains(CFMetaData.Flag.COUNTER)
    val isDense = flags.contains(CFMetaData.Flag.DENSE)
    val isCompound = isView || flags.contains(CFMetaData.Flag.COMPOUND)
    val query = SimpleStatement
      .builder(s"SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.COLUMNS} WHERE keyspace_name = ? AND table_name = ?")
      .addPositionalValues(keyspace, name)
      .build()

    import scala.collection.JavaConversions._
    val defs = new java.util.ArrayList[ColumnDefinition]()
    for (colRow <- session.execute(query)) {
      defs.add(createDefinitionFromRow(colRow, keyspace, name, types))
    }

    CFMetaData.create(keyspace, name, id, isDense, isCompound, isSuper, isCounter, isView, defs, partitioner)
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.createDefinitionFromRow
  private def createDefinitionFromRow(row: Row, keyspace: String, table: String, types: Types): ColumnDefinition = {
    val cdName = ColumnIdentifier.getInterned(row.getByteBuffer("column_name_bytes"), row.getString("column_name"))
    val cdOrder = ClusteringOrder.withName(row.getString("clustering_order").toUpperCase)
    var cdType = CQLTypeParser.parse(keyspace, row.getString("type"), types)
    if (cdOrder eq ClusteringOrder.DESC) cdType = ReversedType.getInstance(cdType)
    val cdPosition = row.getInt("position")
    val cdKind = ColumnDefinition.Kind.valueOf(row.getString("kind").toUpperCase)
    new ColumnDefinition(keyspace, table, cdName, cdType, cdPosition, cdKind)
  }

}

// Cassandra 3.x has ClusteringOrder.NONE, which the datastax driver doesn't know about
object ClusteringOrder extends Enumeration {
  val ASC, DESC, NONE = Value
}

object SparkCassSSTableLoaderClient {
  type TokenRange = org.apache.cassandra.dht.Range[org.apache.cassandra.dht.Token]
}
