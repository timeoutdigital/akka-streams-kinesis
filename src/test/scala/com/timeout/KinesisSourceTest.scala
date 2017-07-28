package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime}
import java.util.Date

import akka.stream.ThrottleMode
import akka.stream.scaladsl.Sink
import com.amazonaws.services.kinesis.model.{DescribeStreamResult, Shard, StreamDescription}
import com.timeout.KinesisSource.{IteratorType, ShardId}
import com.timeout.KinesisSource.IteratorType.{Latest, TrimHorizon}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}
import org.scalatest.tagobjects.Slow

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class KinesisSourceTest extends FreeSpec with Matchers with ScalaFutures with KinesaliteTest {

  val now = ZonedDateTime.parse("2017-01-01T07:00:00Z")
  implicit val clock = Clock.fixed(now.toInstant, now.getZone)
  override implicit val patienceConfig = PatienceConfig(60.seconds, 100.millis)

  val stream = "test"
  val shards = List(
     ShardId("1234"),
     ShardId("2345")
  )

  "shardIteratorRequests function" - {

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
        records <- readN(4)
      } yield records.toList

      whenReady(results) { records =>
        records shouldEqual shardsToRecords.map(_._2)
      }
    }

    "Should work with a stream that has recently been resharded" in {

      val beforeReshard = List(1 -> "a", 2 -> "b", 3 -> "c")
      val afterReshard = (1 to 5).map(1 -> _.toString).toList
      pushToStream(beforeReshard)

      kinesis.describeStream(streamName).getStreamDescription.getShards.asScala.foreach { shard =>
        val hashKey = (BigInt(shard.getHashKeyRange.getStartingHashKey) + 50).toString
        kinesis.splitShard(streamName, shard.getShardId, hashKey)
        Thread.sleep(600)
      }

      pushToStream(afterReshard)
      whenReady(readN(beforeReshard.length + afterReshard.length)) { r =>
        r.sorted shouldEqual (beforeReshard ++ afterReshard).sorted.map(_._2)
      }
    }
  }

  "iteratorForReshard function" - {

    "Should turn LATEST iterators into TRIM_HORIZON for newly created shards" in {
      KinesisSource.iteratorForReshard(IteratorType.Latest) shouldEqual IteratorType.TrimHorizon
    }

    "Should pass through other iterator types unchanged" in {
      KinesisSource.iteratorForReshard(IteratorType.AtTimestamp(now)) shouldEqual IteratorType.AtTimestamp(now)
      KinesisSource.iteratorForReshard(IteratorType.TrimHorizon) shouldEqual IteratorType.TrimHorizon
    }
  }

  "createShards function" - {

    "Should find top level shards of a stream which hasn't been resharded" in {
      val desc = new StreamDescription().withShards(
        new Shard().withShardId("1234"),
        new Shard().withShardId("2345")
      )

      val stream = new DescribeStreamResult()
        .withStreamDescription(desc)

      val result = KinesisSource.findOldestPossibleShards(stream)
      result shouldEqual List(ShardId("1234"), ShardId("2345"))
    }

    "Should find child shards if a parent shard ID is passed in" in {
      val desc = new StreamDescription().withShards(
        // we want to find 1234 as its parent is the shard we want
        new Shard().withShardId("1234").withParentShardId("123"),

        // 2345 is an adjacent child of 123 which we want to ignore
        new Shard().withShardId("2345").withAdjacentParentShardId("123"),

        // 3456 is not a child shard so we want to ignore
        new Shard().withShardId("3456")
      )

      val stream = new DescribeStreamResult()
        .withStreamDescription(desc)

      val result = KinesisSource.findChildShards(stream, parent = ShardId("123"))
      result shouldEqual List(ShardId("1234"))
    }

    "Should consider orphan child shards to be parents" in {
      /*
       * long after a stream is resharded the child shards produced
       * still reference their parent shards, even though they're expired
       * so if we get a child shard pointing to a non existent parent
       * we want to consider it to be a top level parent shard
       */
      val desc = new StreamDescription().withShards(
        new Shard().withShardId("1234").withParentShardId("123"),
        new Shard().withShardId("3456")
      )

      val stream = new DescribeStreamResult()
        .withStreamDescription(desc)

      val result = KinesisSource.findOldestPossibleShards(stream)
      result shouldEqual List(ShardId("1234"), ShardId("3456"))
    }

    "Should exclude shards which have been resharded when finding the newest possible shards" in {
      val desc = new StreamDescription().withShards(
        // we start with one shard
        new Shard().withShardId("1"),

        // we then reshard to split that one shard into two
        new Shard().withShardId("2").withParentShardId("1"),
        new Shard().withShardId("3").withParentShardId("1"),

        // finally we reshard back to one shard, so merge both shards back into one
        new Shard().withShardId("4").withParentShardId("2").withAdjacentParentShardId("3")
      )

      val stream = new DescribeStreamResult()
        .withStreamDescription(desc)

      val result = KinesisSource.findNewestPossibleShards(stream)
      result shouldEqual List(ShardId("4"))
    }
  }

  "Kinesis Source with kinesalite" - {

    "Should work with a stream that is resharded while the source is reading" in {
      val beforeReshard = List(1 -> "a", 2 -> "b", 3 -> "c")
      val afterReshard = (1 to 5).map(1 -> _.toString).toList
      val stream = readN(beforeReshard.length + afterReshard.length)
      pushToStream(beforeReshard)
      splitShards()

      pushToStream(afterReshard)
      whenReady(stream) { r =>
        r.sorted shouldEqual (beforeReshard ++ afterReshard).sorted.map(_._2)
      }
    }

    "Should work with a stream whose shards are merged" in {
      val beforeReshard = List(1 -> "a", 2 -> "b", 3 -> "c")
      val afterReshard = (1 to 5).map(1 -> _.toString).toList
      val stream = readN(beforeReshard.length + afterReshard.length)
      pushToStream(beforeReshard)

      val shards = kinesis.describeStream(streamName).getStreamDescription.getShards.asScala
      kinesis.mergeShards(streamName, shards.head.getShardId, shards(1).getShardId)
      Thread.sleep(600)

      pushToStream(afterReshard)
      whenReady(stream) { r =>
        r.sorted shouldEqual (beforeReshard ++ afterReshard).sorted.map(_._2)
      }
    }

    "Should only get records from the latest shards when reading LATEST" in {
      /*
       * We do two reshards because any detected reshards when reading LATEST
       * will begin reading the new shards from TRIM_HORIZON, so if
       * we get any info from the first two shard sets we know that
       * getNewestPossibleShards isn't working too well
       */
      pushToStream(List(1 -> "a", 2 -> "aa"))
      splitShards()
      pushToStream(List(1 -> "b", 2 -> "bb", 3 -> "bbb", 4 -> "bbbb"))
      splitShards()
      val latestShardContents = (1 to 8).map(i => i -> List.fill(i)("c").mkString).toList
      val reading = readN(number = 8, it = Latest)
      Thread.sleep(500) // wait for describeStream etc to finish
      pushToStream(latestShardContents)
      whenReady(reading) { data =>
        data.sorted shouldEqual latestShardContents.map(_._2)
      }
    }

    "Should reissue iterators after five minutes of inactivity" taggedAs Slow in {
      pushToStream(List(1 -> "a", 1 -> "aa"))
      val reading = readDelayed
      whenReady(reading, timeout = Timeout(10.minutes)) { data =>
        data.sorted shouldEqual Vector("a", "aa")
      }
    }
  }

  /**
    * Read a certain number of records from Kinesis
    */
  private def readN(
    number: Int,
    it: IteratorType = TrimHorizon,
    timeOut: Option[FiniteDuration] = None
  ): Future[Seq[String]] =
    KinesisSource(kinesis, streamName, it)
      .throttle(1, 2.second, 1, ThrottleMode.shaping) // https://github.com/mhart/kinesalite/pull/36
      .map(b => new String(b.array))
      .groupedWithin(number * 2, timeOut.getOrElse((number * 2).seconds))
      .runWith(Sink.head)

  /**
    * Read two records from Kinesis, backpressuring
    * for six minutes to expire the shard iterator
    */
  private def readDelayed: Future[Seq[String]] = {
    val allowThroughAt = ZonedDateTime.now().plusMinutes(6)
    KinesisSource(kinesis, streamName, TrimHorizon)
      .map(b => new String(b.array))
      .mapAsync(parallelism = 1) { b =>
        val p = Promise[String]()
        val delay = Math.max(allowThroughAt.toEpochSecond - ZonedDateTime.now.toEpochSecond, 0)
        as.scheduler.scheduleOnce(delay.seconds)(p.success(b))
        p.future
      }.groupedWithin(2, 7.minutes)
      .runWith(Sink.head)
  }
}
