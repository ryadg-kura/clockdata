name := "alert-service"
version := "0.1"
scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.7.0",
  "com.lihaoyi" %% "upickle" % "3.3.1",
  "com.sun.mail" % "jakarta.mail" % "2.0.1",
  "org.scalameta" %% "munit" % "1.0.0" % Test
)

testFrameworks += new TestFramework("munit.Framework")

fork := true
