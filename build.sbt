name := "akka-streams-kinesis"

organization := "com.timeout"

licenses += ("MIT", url("https://opensource.org/licenses/MIT"))

scalaVersion := "2.13.2"

releaseCrossBuild := true

libraryDependencies += "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.808"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.30" % "test"

credentials += Credentials(Path.userHome / ".bintray" / ".credentials")

resolvers += Resolver.bintrayRepo("timeoutdigital", "releases")

lazy val root = (project in file("."))
  .settings(BintrayPlugin.bintrayPublishSettings: _*)
