resolvers += "Typesafe Repository"   at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "sonatype-releases"     at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"
resolvers += "Teads Releases" at "http://nexus.teads.net/content/repositories/releases"
resolvers += "Teads Snapshots" at "http://nexus.teads.net/content/repositories/snapshots"

addSbtPlugin("tv.teads" % "teads-build-plugin" % "6.0.3")
addSbtPlugin("org.scalariform"   % "sbt-scalariform" % "1.6.0")
addSbtPlugin("com.jsuereth"      % "sbt-pgp"         % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release"     % "1.0.2")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"   % "1.3.5")
addSbtPlugin("com.eed3si9n"      % "sbt-assembly"    % "0.14.3")
