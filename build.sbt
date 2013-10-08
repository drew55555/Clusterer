import AssemblyKeys._

assemblySettings

jarName in assembly := "Clusterer.jar"

name := "Clusterer"

version := "0.1"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.mongodb" %% "casbah" % "2.6.3",
  "org.slf4j" % "slf4j-simple" % "1.6.4"
)
