import sbt._

class PrequelProject(info: ProjectInfo) extends DefaultProject( info ) {

    val commonsPool = "commons-pool" % "commons-pool" % "1.5.5"
    val commonsDbcp = "commons-dbcp" % "commons-dbcp" % "1.4"
    val commonsLang = "commons-lang" % "commons-lang" % "2.6"
    val jodaTime = "joda-time" % "joda-time" % "1.6.2"  
    
    // Testing Dependencies
    val hsqldb = "org.hsqldb" % "hsqldb" % "2.0.0" % "test"
    val scalaTest = "org.scalatest" % "scalatest" % "1.3" % "test"
    
    val scalaToolsReleases = "Scala-Tools Releases" at "http://scala-tools.org/repo-releases"
}
