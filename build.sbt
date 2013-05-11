name := "prequel"

version := "0.3.8"

organization := "net.noerd"

scalaVersion := "2.10.0"

// Runtime Dependencies
libraryDependencies ++= Seq(
    "commons-pool" % "commons-pool" % "1.5.5",
    "commons-dbcp" % "commons-dbcp" % "1.4",
    "commons-lang" % "commons-lang" % "2.6",
    "joda-time" % "joda-time" % "2.1",
    "org.joda" % "joda-convert" % "1.2"
)

// Test Dependencies
libraryDependencies ++= Seq(
    "org.hsqldb" % "hsqldb" % "2.2.4" % "test",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)

// Release publishing stuff

publishTo := Some( "Sonatype Staging Nexus" at "https://oss.sonatype.org/service/local/staging/deploy/maven2" )

pomIncludeRepository := { _ => false }

publishMavenStyle := true

credentials += Credentials( Path.userHome / ".ivy2" / ".credentials" )

pomExtra := (
  <url>http://github.com/jpersson/prequel</url>
  <licenses>
    <license>
      <name>wtfpl</name>
      <url>http://www.wtfpl.net/txt/copying/</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/jpersson/prequel.git</url>
    <connection>scm:git:https://github.com/jpersson/prequel.git</connection>
  </scm>
  <developers>
    <developer>
      <id>jpersson</id>
      <name>Johan Persson</name>
      <url>http://github.com/jpersson</url>
    </developer>
  </developers>
)
