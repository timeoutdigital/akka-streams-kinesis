name := "akka-streams-kinesis"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.60"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.4.14"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"
