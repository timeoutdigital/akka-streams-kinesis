# Akka Streams Kinesis

[![Build Status](https://travis-ci.org/timeoutdigital/akka-streams-kinesis.svg?branch=master)](https://travis-ci.org/timeoutdigital/akka-streams-kinesis)
[![GitHub release](https://img.shields.io/github/tag/timeoutdigital/akka-streams-kinesis.svg)](https://github.com/timeoutdigital/akka-streams-kinesis/releases)

This library allows you to read from and write to Kinesis with Akka Streams. It is currently 
being used in Time Out's data ingestion pipeline and is under active development.

## Installation

Add the following to your `build.sbt`:

```scala
resolvers += Resolver.bintrayRepo("timeoutdigital", "releases")
libraryDependencies += "com.timeout" %% "akka-streams-kinesis" % "0.1.6"
```

## Reading from Kinesis

You can read from Kinesis with `KinesisSource`, which uses the `AmazonKinesisAsync` client internally. 

```scala
import com.timeout.KinesisSource
import java.time.ZonedDateTime

val since = ZonedDateTime.now // from when to start reading the stream
val stage = KinesisSource("my-stream-name", since = since)
```

⚠️ Currently not supported:

 - Shard iterator types other than `AT_TIMESTAMP`
 - [Resharding of the stream](http://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding.html)
 
## Writing to Kinesis

You can write to Kinesis with `KinesisGraphStage`. It maintains an internal buffer of records which it flushes periodically to Kinesis. and emits a stream of  `Either[PutRecordsResultEntry, A]` where the left is any failed results from Kinesis and the right is the records pushed successfully.

`KinesisGraphStage` expects you to implement a typeclass `ToPutRecordsRequest[A]` which tells it how to convert the contents of your stream into a `PutRecordsRequest`.

Here's an example of how to write a Stream of `String`s to Kinesis:

```scala
import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.timeout.{KinesisGraphStage, ToPutRecordsRequest}
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

implicit val instance = ToPutRecordsRequest.instance[String] { str =>
  new PutRecordsRequestEntry().withData(ByteBuffer.wrap(str.getBytes))
}

val kinesis = new AmazonKinesisAsyncClient()
val stage = KinesisGraphStage.withClient[String](kinesis, "my-stream")
// stage: Flow[String, Either[PutRecordsResultEntry,String], NotUsed]
```
