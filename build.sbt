name := "prequel"

version := "0.3.5"

organization := "net.noerd"

scalaVersion := "2.9.0"

// Runtime Dependencies
libraryDependencies ++= Seq(
    "commons-pool" % "commons-pool" % "1.5.5",
    "commons-dbcp" % "commons-dbcp" % "1.4",
    "commons-lang" % "commons-lang" % "2.6",
    "joda-time" % "joda-time" % "1.6.2"
)

// Test Dependencies
libraryDependencies ++= Seq(
    "org.hsqldb" % "hsqldb" % "2.0.0" % "test",
    "org.scalatest" %% "scalatest" % "1.4.1" % "test"
)
