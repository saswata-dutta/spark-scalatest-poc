lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.sasdutta",
      scalaVersion := "2.12.12"
    )),
    name := "spark-scalatest-poc",
    version := "0.0.1"

  )


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

// test suite settings
fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

