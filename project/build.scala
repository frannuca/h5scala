import sbt._
import Keys._


//import com.github.retronym.SbtOneJar

object BuildSettings {


  val buildOrganization = "org.fjn"
  val buildVersion      = "1.0.0"
  val buildScalaVersion = "2.10.3"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion
  )
}

object Resolvers {

  val typesafe = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
  val sonatype1 = "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"
  val sonatype2 = "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
}

object Dependencies {

  //val h5 = "org.scala-saddle" % "jhdf5" % "2.9"
  val junit = "junit" % "junit" % "4.11"
  val scalaTest = "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

  val scalaReflect = "org.scala-lang" % "scala-reflect" % BuildSettings.buildScalaVersion
  val scalaCompiler =  "org.scala-lang" % "scala-compiler"% BuildSettings.buildScalaVersion
  val log4j = "log4j" % "log4j" % "1.2.14"
}

object ProjectBuild extends Build {


  import Resolvers._
  import Dependencies._
  import BuildSettings._

  /**
   * top layer  pythia
   */
  lazy val p = Project (
    "h5scala",
    file ("."),
    settings = buildSettings++ Seq (resolvers :=  Seq(), libraryDependencies ++=Seq(log4j,junit,scalaTest,scalaCompiler,scalaReflect))

  ) //aggregate (optimizer,ia, fjn.fjn.fjn.pythia.pricers)



}
