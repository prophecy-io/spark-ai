ThisBuild / scalaVersion := "2.12.17"

lazy val hello = (project in file("."))
  .settings(
    name := "spark-ai",
    assemblyPackageScala / assembleArtifact := false
  )

libraryDependencies += "com.squareup.okhttp3" % "okhttp" % "4.10.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.2" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % Test
libraryDependencies += "io.github.cdimascio" % "dotenv-java" % "2.3.2" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2" % Test
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.2" % Test

ThisBuild / assemblyMergeStrategy  := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

ThisBuild / organization := "io.prophecy"
ThisBuild / organizationName := "Prophecy"
ThisBuild / organizationHomepage := Some(url("https://www.prophecy.io"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/prophecy-io/spark-ai"),
    "scm:git@github.com:prophecy-io/spark-ai.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id    = "several27",
    name  = "Maciej Szpakowski",
    email = "maciej@prophecy.io",
    url   = url("https://www.prophecy.io")
  )
)

ThisBuild / description := "A code-bapublishSignedsed framework for building Generative AI applications on top of Apache Spark."
ThisBuild / licenses := List("The Unlicense" -> new URL("https://unlicense.org/"))
ThisBuild / homepage := Some(url("https://prophecy.io"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

import com.jsuereth.sbtpgp.PgpKeys.gpgCommand
Global / gpgCommand := {
  val sh = (baseDirectory.value / "gpg.sh").getAbsolutePath
  sh
}

ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

ThisBuild / publishMavenStyle := true

ThisBuild / versionScheme := Some("early-semver")