package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime}
import java.util.Date

import akka.stream.scaladsl.Sink
import com.amazonaws.services.kinesis.model.{Shard, StreamDescription, UpdateShardCountRequest}
import com.timeout.KinesisSource.IteratorType
import com.timeout.KinesisSource.IteratorType.TrimHorizon
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.duration._

class KinesisSourceTest extends
  FreeSpec with Matchers with ScalaFutures with KinesaliteTest {

  val now = ZonedDateTime.parse("2017-01-01T07:00:00Z")
  implicit val clock = Clock.fixed(now.toInstant, now.getZone)
  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.millis)

  val stream: StreamDescription = new StreamDescription()
    .withStreamName("test name")
    .withShards(
      new Shard().withShardId("1234"),
      new Shard().withShardId("2345")
    )

  "Kinesis Source" - {

    "Should generate one AT_TIMESTAMP iterator request per shard" in {
      val requests = KinesisSource.shardIteratorRequests(IteratorType.AtTimestamp(now), stream)
      requests.map(_.getShardId).toSet shouldEqual Set("1234", "2345")
      requests.map(_.getShardIteratorType).toSet shouldEqual Set("AT_TIMESTAMP")
      requests.map(_.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }

    "Should cap the timestamp of shard iterator requests to the minimum of (now, since)" in {
      val requests = KinesisSource.shardIteratorRequests(IteratorType.AtTimestamp(now.plusDays(1)), stream)
      requests.map(_.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }

    "Should map IteratorType.Latest to LATEST" in {
      val latest = KinesisSource.shardIteratorRequests(IteratorType.Latest, stream)
      latest.head.getShardIteratorType shouldEqual "LATEST"
      latest.head.getStartingSequenceNumber shouldEqual null
      latest.head.getTimestamp shouldEqual null
    }

    "Should map IteratorType.TrimHorizon to TRIM_HORIZON" in {
      val latest = KinesisSource.shardIteratorRequests(IteratorType.TrimHorizon, stream)
      latest.head.getShardIteratorType shouldEqual "TRIM_HORIZON"
      latest.head.getStartingSequenceNumber shouldEqual null
      latest.head.getTimestamp shouldEqual null
    }

    "Should read all the records pushed to a shard in the right order" in {

      // give all the strings the same partition key of 1
      val shardsToRecords = List(1 -> "a", 1 -> "b", 1 -> "c", 1 -> "d")

      val results = for {
        _ <- pushToStream(shardsToRecords)
        records <- KinesisSource(kinesis, streamName, TrimHorizon).take(4).runWith(Sink.seq)
      } yield records.toList

      whenReady(results) { records =>
        records.map(b => new String(b.array)) shouldEqual shardsToRecords.map(_._2)
      }
    }

    // todo this should not pass - we don't handle null iterators
    // is kinesalite not returning null shard iterators?
    "Should work with a stream that has recently been resharded" in {

      // so first shove some records onto the old shards
      pushToStream(List(1 -> "a", 2 -> "b", 3 -> "c"))
      val shard = kinesis.describeStream(streamName).getStreamDescription.getShards.get(0)
      kinesis.splitShard(streamName, shard.getShardId, (BigInt(shard.getHashKeyRange.getEndingHashKey) / 2).toString)
      Thread.sleep(500)
      pushToStream(List(1 -> "d", 2 -> "e", 3 -> "f", 4 -> "g"))
      Thread.sleep(500)

      whenReady(KinesisSource(kinesis, streamName, TrimHorizon).take(7).runWith(Sink.seq[ByteBuffer])) { r =>
        r.map(b => new String(b.array)).toSet shouldEqual Set("a", "b", "c", "d", "e", "f", "g")
      }
    }
  }
}
