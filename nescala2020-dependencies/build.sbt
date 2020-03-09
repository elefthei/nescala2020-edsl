import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "com.bwater"
ThisBuild / organizationName := "nescala"

libraryDependencies += "joda-time" % "joda-time" % "2.10.5"
libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.0"
lazy val root = (project in file("."))
  .settings(
    name := "nescala2020-dependencies",
    libraryDependencies += scalaTest % Test
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
