// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "com.github.leoromanovsky"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// License of your choice
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/leoromanovsky/Spark2Cassandra"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/leoromanovsky/Spark2Cassandra"),
    "scm:git@github.com:leoromanovsky/Spark2Cassandra.git"
  )
)
developers := List(
  Developer(
    id="leoromanovsky",
    name="Leo Romanovsky",
    email="leoromanovsky@gmail.com",
    url=url("http://leoromanovsky.com")
  )
)
