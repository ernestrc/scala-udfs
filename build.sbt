import sbt.Tests.Setup

name := """scala-udfs"""

version := "0.2"

scalaVersion := "2.11.6"

javacOptions := Seq("-target", "1.6", "-source", "1.6", "-Xlint:-options")

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-target:jvm-1.6")

resolvers += "Cascading repo" at "http://conjars.org/repo"

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.1.1" % "provided",
  "org.apache.hive" % "hive-exec" % "0.9.0" % "provided",
  "com.google.protobuf" % "protobuf-java" % "2.4.1",
  "joda-time" % "joda-time" % "2.8.1",
  "org.joda" % "joda-convert" % "1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test"
)
