package com.timeout

import java.time.{Clock, ZonedDateTime}
import java.util.Date

import com.amazonaws.services.kinesis.model.{Shard, StreamDescription}
import org.scalatest.{FreeSpec, Matchers}

class KinesisSourceTest extends FreeSpec with Matchers {
  val now = ZonedDateTime.parse("2017-01-01T07:00:00Z")
  implicit val clock = Clock.fixed(now.toInstant, now.getZone)

  val stream: StreamDescription = new StreamDescription()
    .withStreamName("test name")
    .withShards(
      new Shard().withShardId("1234"),
      new Shard().withShardId("2345")
    )

  "Kinesis Source" - {

    "Should generate one AT_TIMESTAMP iterator request per shard" in {
      val requests = KinesisSource.shardIteratorRequests(now, stream)
      requests.map(_.getShardId).toSet shouldEqual Set("1234", "2345")
      requests.map(_.getShardIteratorType).toSet shouldEqual Set("AT_TIMESTAMP")
      requests.map(_.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }

    "Should cap the timestamp of shard iterator requests to the minimum of (now, since)" in {
      val requests = KinesisSource.shardIteratorRequests(now.plusDays(1), stream)
      requests.map(_.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }
  }
}
