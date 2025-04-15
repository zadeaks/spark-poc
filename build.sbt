ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "3.5.5","org.apache.spark" %% "spark-core" % "3.5.5"
, "com.typesafe" % "config" % "1.4.3")

lazy val root = (project in file("."))
  .settings(
    name := "spark-poc"
  )
