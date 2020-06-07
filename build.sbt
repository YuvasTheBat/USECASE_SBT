name := "ICA_POC"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0"
)

//ScalaTest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// https://mvnrepository.com/artifact/junit/junit
libraryDependencies += "junit" % "junit" % "4.11" % Test

coverageMinimum := 60
coverageFailOnMinimum := true

