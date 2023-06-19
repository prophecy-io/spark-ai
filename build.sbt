
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "io_prophecy"

lazy val hello = (project in file("."))
  .settings(
    name := "spark-ai",
    assemblyPackageScala / assembleArtifact := false
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.4.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % Test
libraryDependencies += "io.github.cdimascio" % "dotenv-java" % "3.0.0" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0" % Test
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.4.0" % Test
