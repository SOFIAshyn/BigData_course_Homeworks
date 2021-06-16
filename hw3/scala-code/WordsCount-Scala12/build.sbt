scalaVersion := "2.12.13"

name := "WordsCount-Scala12"
organization := "scala.example"
version := "0.1"


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
  "org.apache.spark" %% "spark-core" % "3.1.2" % "compile"
)