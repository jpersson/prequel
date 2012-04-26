name := "prequel"

version := "0.3.8"

organization := "net.noerd"

scalaVersion := "2.9.2"

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
    "org.scalatest" %% "scalatest" % "1.7.2" % "test"
)
