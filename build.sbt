import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

/**
  * Organization:
  */
organization     := "com.github.leoromanovsky"
organizationName := "leoromanovsky"

/**
  * Library Meta:
  */
name     := "Spark2Cassandra"
licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

/**
  * Scala:
  */
scalaVersion       := "2.11.8"
crossScalaVersions := Seq("2.11.8")

/**
  * Library Dependencies:
  */

// Versions:
val SparkVersion                   = "2.2.0"
val SparkTestVersion               = "2.2.0_0.7.2"
val ScalaTestVersion               = "3.0.4"
val LogVersion                     = "1.3.0"
val SparkCassandraConnectorVersion = "2.0.5"
val CassandraAllVersion            = "3.9"
val CassandraUnitVersion           = "3.1.1.0"

// Dependencies:
val sparkCore       = "org.apache.spark"     %% "spark-core"                % SparkVersion                   % "provided"
val sparkSql        = "org.apache.spark"     %% "spark-sql"                 % SparkVersion                   % "provided"
val sparkTest       = "com.holdenkarau"      %% "spark-testing-base"        % SparkTestVersion               % "test"
val scalaTest       = "org.scalatest"        %% "scalatest"                 % ScalaTestVersion               % "test"
val logger          = "org.clapper"          %% "grizzled-slf4j"            % LogVersion
val ssc             = "com.datastax.spark"   %% "spark-cassandra-connector" % SparkCassandraConnectorVersion
val cassandraAll    = "org.apache.cassandra" %  "cassandra-all"             % CassandraAllVersion
val cassandraClient = "org.apache.cassandra" %  "cassandra-clientutil"      % CassandraAllVersion
val cassandraUnit   = "org.cassandraunit"    %  "cassandra-unit"            % CassandraUnitVersion           % "test"

libraryDependencies ++= Seq(sparkCore, sparkSql, sparkTest, scalaTest, logger, ssc, cassandraAll, cassandraUnit)

excludeDependencies += "org.slf4j" % "slf4j-log4j12"

// Force cassandraUnit and ssc to utilize cassandraAll, cassandraClient.
dependencyOverrides ++= Set(cassandraAll, cassandraClient)

/**
  * Tests:
  */
parallelExecution in Test := false

/**
  * Scalariform:
  */
SbtScalariform.scalariformSettings
ScalariformKeys.preferences := FormattingPreferences()
  .setPreference(RewriteArrowSymbols, false)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(SpacesAroundMultiImports, true)

/**
  * Scoverage:
  */
coverageEnabled in Test := true

/**
  * Assembly: some upgrades may be problematic
  */
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("io.netty.**" -> "s2c.netty.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "s2c.google.common.@1").inAll,
  ShadeRule.rename("com.google.thirdparty.publicsuffix.**" -> s"s2c.google.thirdparty.publicsuffix.@1").inAll
)

/**
  * Release:
  * https://github.com/xerial/sbt-sonatype
  */
import ReleaseTransformations._

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  setNextVersion,
  commitNextVersion,
  releaseStepCommand("sonatypeReleaseAll"),
  pushChanges
)
