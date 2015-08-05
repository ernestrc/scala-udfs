import sbt._
import Keys._


object UDFsBuild extends Build {

  val scalaV = "2.11.6"

  val commonSettings = Seq(
    organization := "com.opentok",
    name := """scala-udfs""",
    version := "0.2.0",
    scalaVersion := scalaV,
    javacOptions := Seq(
      "-target", "1.6", 
      "-source", "1.6", 
      "-Xlint:-options"   
    ),
    resolvers ++= Seq(
      "Cascading repo" at "http://conjars.org/repo",
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases" at "http://oss.sonatype.org/content/repositories/releases",
      "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
    ),
    // Enables publishing to maven repo
    publishMavenStyle := true,
    // Do not append Scala versions to the generated artifacts
    crossPaths := false,
    // This forbids including Scala related libraries into the dependency
    autoScalaLibrary := false,
    scalacOptions := Seq(
      "-unchecked",
      "-Xlog-free-terms",
      "-deprecation",
      "-encoding", "UTF-8",
      "-target:jvm-1.6"
    )
)

  lazy val surus: Project = Project("netflix-surus", file("netflix-surus"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "org.jpmml" % "pmml-evaluator" % "1.0.19", 
        "org.jpmml" % "pmml-model" % "1.0.19", 
        "junit" % "junit-dep" % "4.8.2" % "test", 
        "org.apache.hadoop" % "hadoop-core" % "1.0.3" % "provided", 
        "org.apache.pig" % "pig" % "0.14.0" % "provided"
      )
    )

    lazy val core: Project = Project("core", file("core"))
      .settings(commonSettings: _*)
      .settings(

        libraryDependencies ++= 
          Seq(
            "org.apache.hadoop" % "hadoop-core" % "1.1.1" % "provided",
            "org.apache.hive" % "hive-exec" % "0.9.0" % "provided",
            "com.google.protobuf" % "protobuf-java" % "2.4.1",
            "joda-time" % "joda-time" % "2.8.1",
            "org.joda" % "joda-convert" % "1.7",
            "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
            "ch.qos.logback" % "logback-classic" % "1.1.3",
            "org.scalatest" %% "scalatest" % "2.2.5" % "test"
          )
        ).dependsOn(surus % "compile->compile;test->test")

      lazy val root: Project = Project("root", file(".")).aggregate(surus, core)
}
