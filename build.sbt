ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "graphDSLBypass"
  )

val akkaVersion = "2.6.20"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion