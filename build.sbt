name := "spark-model-tutorial"

organization := "org.kr.spark.tutorial"

version := "0.1"

scalaVersion := "2.12.15"

val sparkVersion = "3.1.2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.12" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies
