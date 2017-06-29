package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime}
import java.util.Date

import akka.stream.ThrottleMode
import akka.stream.scaladsl.Sink
import com.timeout.KinesisSource.IteratorType
import com.timeout.KinesisSource.IteratorType.TrimHorizon
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class KinesisSourceTest extends
  FreeSpec with Matchers with ScalaFutures with KinesaliteTest {

  val now = ZonedDateTime.parse("2017-01-01T07:00:00Z")
  implicit val clock = Clock.fixed(now.toInstant, now.getZone)
  override implicit val patienceConfig = PatienceConfig(30.seconds, 100.millis)

  val stream = "test"
  val shards = List(
     Shard("1234", List.empty),
     Shard("2345", List.empty)
  )

  "Kinesis Source" - {

    "Should generate one AT_TIMESTAMP iterator request per shard" in {
      val requests = KinesisSource.shardIteratorRequests(IteratorType.AtTimestamp(now), shards, stream)
      requests.map(_._2.getShardId).toSet shouldEqual Set("1234", "2345")
      requests.map(_._2.getShardIteratorType).toSet shouldEqual Set("AT_TIMESTAMP")
      requests.map(_._2.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }

    "Should cap the timestamp of shard iterator requests to the minimum of (now, since)" in {
      val requests = KinesisSource.shardIteratorRequests(IteratorType.AtTimestamp(now.plusDays(1)), shards, stream)
      requests.map(_._2.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }

    "Should map IteratorType.Latest to LATEST" in {
      val latest = KinesisSource.shardIteratorRequests(IteratorType.Latest, shards, stream)
      latest.head._2.getShardIteratorType shouldEqual "LATEST"
      latest.head._2.getStartingSequenceNumber shouldEqual null
      latest.head._2.getTimestamp shouldEqual null
    }

    "Should map IteratorType.TrimHorizon to TRIM_HORIZON" in {
      val latest = KinesisSource.shardIteratorRequests(IteratorType.TrimHorizon, shards, stream)
      latest.head._2.getShardIteratorType shouldEqual "TRIM_HORIZON"
      latest.head._2.getStartingSequenceNumber shouldEqual null
      latest.head._2.getTimestamp shouldEqual null
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

    "Should work with a stream that has recently been resharded" in {

      val beforeReshard = List(1 -> "a", 2 -> "b", 3 -> "c")
      val afterReshard = (1 to 100).map(1 -> _.toString).toList
      pushToStream(beforeReshard)

      kinesis.describeStream(streamName).getStreamDescription.getShards.asScala.foreach { shard =>
        val hashKey = (BigInt(shard.getHashKeyRange.getStartingHashKey) + 50).toString
        kinesis.splitShard(streamName, shard.getShardId, hashKey)
        Thread.sleep(600)
      }

      pushToStream(afterReshard)
      whenReady(KinesisSource(kinesis, streamName, TrimHorizon).take(beforeReshard.length + afterReshard.length).runWith(Sink.seq[ByteBuffer])) { r =>
        r.map(b => new String(b.array)).toSet shouldEqual (beforeReshard.toSet ++ afterReshard.toSet).map(_._2)
      }
    }

    "Should work with a stream that is resharded while the source is reading" in {
      val beforeReshard = List(1 -> "a", 2 -> "b", 3 -> "c")
      val afterReshard = (1 to 10).map(1 -> _.toString).toList
      val stream = KinesisSource(kinesis, streamName, TrimHorizon)
        .throttle(1, 1.second, 1, ThrottleMode.shaping) // https://github.com/mhart/kinesalite/pull/36
        .take(beforeReshard.length + afterReshard.length)
        .runWith(Sink.seq[ByteBuffer])
      pushToStream(beforeReshard)

      kinesis.describeStream(streamName).getStreamDescription.getShards.asScala.foreach { shard =>
        val hashKey = (BigInt(shard.getHashKeyRange.getStartingHashKey) + 50).toString
        kinesis.splitShard(streamName, shard.getShardId, hashKey)
        Thread.sleep(600)
      }

      pushToStream(afterReshard)
      whenReady(stream) { r =>
        val recordSet = r.map(b => new String(b.array)).toSet
        recordSet shouldEqual (beforeReshard.toSet ++ afterReshard.toSet).map(_._2)
      }
    }
  }
}
