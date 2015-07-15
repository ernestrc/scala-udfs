name := """hive-udfs"""

version := "0.1"

scalaVersion := "2.11.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += "Cascading repo" at "http://conjars.org/repo"

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases",
  "cloudera"  at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies += "org.apache.hive" % "hive-exec" % "0.9.0" % "provided"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.1.1" % "provided"
)
