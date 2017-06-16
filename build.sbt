name := "akka-streams-kinesis"

organization := "com.timeout"

licenses += ("MIT", url("https://opensource.org/licenses/MIT"))

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.12.2")

releaseCrossBuild := true

libraryDependencies += "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.60"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.4.17"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.23" % "test"

credentials += Credentials(Path.userHome / ".bintray" / ".credentials")

lazy val root = (project in file("."))
  .settings(BintrayPlugin.bintrayPublishSettings: _*)
  .settings(Seq(
    bintrayOrganization := Some("timeoutdigital"),
    bintrayRepository := "releases"
  ))
