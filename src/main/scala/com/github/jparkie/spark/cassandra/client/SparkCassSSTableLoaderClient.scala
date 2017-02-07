package com.github.jparkie.spark.cassandra.client

import com.datastax.driver.core.{ Metadata, Row, Session }
import com.github.jparkie.spark.cassandra.conf.SparkCassServerConf
import org.apache.cassandra.config.{ CFMetaData, ColumnDefinition }
import org.apache.cassandra.cql3.ColumnIdentifier
import org.apache.cassandra.db.marshal.ReversedType
import org.apache.cassandra.dht.IPartitioner
import org.apache.cassandra.io.sstable.SSTableLoader
import org.apache.cassandra.schema.{ CQLTypeParser, SchemaKeyspace, Types }
import org.apache.cassandra.streaming.StreamConnectionFactory
import org.apache.cassandra.tools.BulkLoadConnectionFactory
import org.apache.cassandra.utils.FBUtilities

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Coordinates the streaming of SSTables to the proper Cassandra data nodes through automated column family discovery.
 *
 * Warning: This borrows heavily from [[org.apache.cassandra.utils.NativeSSTableLoaderClient]]
 *
 * @param session A prolonged [[Session]] to query system keyspaces.
 * @param sparkCassServerConf Configurations to connect to Cassandra Transport Layer.
 */
class SparkCassSSTableLoaderClient(
  val session:             Session,
  val sparkCassServerConf: SparkCassServerConf
) extends SSTableLoader.Client {
  import SparkCassSSTableLoaderClient._

  private[client] val tables: mutable.Map[String, CFMetaData] = mutable.HashMap.empty[String, CFMetaData]

  override def init(keyspace: String): Unit = {
    val cluster = session.getCluster

    val metadata = cluster.getMetadata
    val partitioner = FBUtilities.newPartitioner(metadata.getPartitioner)

    val tokenRanges = metadata.getTokenRanges.asScala
    val tokenFactory = partitioner.getTokenFactory

    for (tokenRange <- tokenRanges) {
      val endpoints = metadata.getReplicas(Metadata.quote(keyspace), tokenRange).asScala

      val range = new TokenRange(
        tokenFactory.fromString(tokenRange.getStart.getValue.toString),
        tokenFactory.fromString(tokenRange.getEnd.getValue.toString)
      )

      for (endpoint <- endpoints) addRangeForEndpoint(range, endpoint.getAddress)
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

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.fetchTypes
  private def fetchTypes(keyspace: String, session: Session): Types = {
    val query = s"SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.TYPES} WHERE keyspace_name = ?"
    val types = Types.rawBuilder(keyspace)
    import scala.collection.JavaConversions._
    for (row <- session.execute(query, keyspace)) {
      val name = row.getString("type_name")
      val fieldNames = row.getList("field_names", classOf[String])
      val fieldTypes = row.getList("field_types", classOf[String])
      types.add(name, fieldNames, fieldTypes)
    }
    types.build
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.fetchTables
  private def fetchTables(keyspace: String, session: Session, partitioner: IPartitioner, types: Types): Unit = {
    val query = s"SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.TABLES} WHERE keyspace_name = ?"
    import scala.collection.JavaConversions._
    for (row <- session.execute(query, keyspace)) {
      val name = row.getString("table_name")
      tables.put(name, createTableMetadata(keyspace, session, partitioner, false, row, name, types))
    }
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.fetchViews
  private def fetchViews(keyspace: String, session: Session, partitioner: IPartitioner, types: Types): Unit = {
    val query = s"SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.VIEWS} WHERE keyspace_name = ?"
    import scala.collection.JavaConversions._
    for (row <- session.execute(query, keyspace)) {
      val name = row.getString("view_name")
      tables.put(name, createTableMetadata(keyspace, session, partitioner, true, row, name, types))
    }
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.createTableMetadata
  private def createTableMetadata(keyspace: String, session: Session, partitioner: IPartitioner, isView: Boolean, row: Row, name: String, types: Types): CFMetaData = {
    val id = row.getUUID("id")

    val flags =
      if (isView) Set.empty[CFMetaData.Flag]
      else CFMetaData.flagsFromStrings(row.getSet("flags", classOf[String])).asScala

    val isSuper = flags.contains(CFMetaData.Flag.SUPER)
    val isCounter = flags.contains(CFMetaData.Flag.COUNTER)
    val isDense = flags.contains(CFMetaData.Flag.DENSE)
    val isCompound = isView || flags.contains(CFMetaData.Flag.COMPOUND)
    val columnsQuery = s"SELECT * FROM ${SchemaKeyspace.NAME}.${SchemaKeyspace.COLUMNS} WHERE keyspace_name = ? AND table_name = ?"

    import scala.collection.JavaConversions._
    val defs = new java.util.ArrayList[ColumnDefinition]()
    for (colRow <- session.execute(columnsQuery, keyspace, name)) {
      defs.add(createDefinitionFromRow(colRow, keyspace, name, types))
    }

    CFMetaData.create(keyspace, name, id, isDense, isCompound, isSuper, isCounter, isView, defs, partitioner)
  }

  // See org.apache.cassandra.utils.NativeSSTableLoaderClient.createDefinitionFromRow
  private def createDefinitionFromRow(row: Row, keyspace: String, table: String, types: Types): ColumnDefinition = {
    val cdName = ColumnIdentifier.getInterned(row.getBytes("column_name_bytes"), row.getString("column_name"))
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
