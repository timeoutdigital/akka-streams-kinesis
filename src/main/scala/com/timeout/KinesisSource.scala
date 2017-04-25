package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime}
import java.util.Date
import java.util.concurrent.{Future => JavaFuture}

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, OutHandler, StageLogging, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object KinesisSource {

  private [timeout] val region: Regions =
    Option(Regions.getCurrentRegion)
      .map(r => Regions.fromName(r.getName))
      .getOrElse(Regions.EU_WEST_1)

  private[timeout] lazy val kinesis: AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.standard.withRegion(region).build

  /**
    * This creates a source that reads records from AWS Kinesis.
    * It is serialisation format agnostic so emits a stream of ByteBuffers
    */
  def apply(
    stream: String,
    since: ZonedDateTime
  )(
    implicit
    ec: ExecutionContext,
    clock: Clock = Clock.systemUTC
  ): Source[ByteBuffer, NotUsed] =
    Source.fromGraph(new KinesisSource(stream, since))

  /**
    * Construct shard iterator requests
    * based on a stream description
    */
  private[timeout] def shardIteratorRequests(
    since: ZonedDateTime,
    stream: StreamDescription
  )(
    implicit
    clock: Clock
  ): List[GetShardIteratorRequest] =
    stream.getShards.asScala.toList.map { shard =>
    val now = clock.instant
    val readFrom = if (since.toInstant.isBefore(now)) since.toInstant else now
    new GetShardIteratorRequest()
      .withShardIteratorType("AT_TIMESTAMP")
      .withTimestamp(Date.from(readFrom))
      .withStreamName(stream.getStreamName)
      .withShardId(shard.getShardId)
  }
}

/**
  * A source for kinesis records
  * For the stream we maintain a map of current iterator => Future[GetRecordsResponse]
  * and we poll the map every 100ms to send more requests for every completed future
  */
private[timeout] class KinesisSource(
  streamName: String,
  since: ZonedDateTime
)(
  implicit
  val e: ExecutionContext,
  clock: Clock
) extends GraphStage[SourceShape[ByteBuffer]] {

  import KinesisSource._
  val outlet = Outlet[ByteBuffer]("Kinesis Records")
  override def shape = SourceShape[ByteBuffer](outlet)

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) with StageLogging {

    // stores futures from kinesis get records requests
    val buffer = mutable.Map.empty[String, JavaFuture[GetRecordsResult]]

    Future {
      val stream = kinesis.describeStream(streamName).getStreamDescription
      shardIteratorRequests(since, stream).toStream.par
        .map(kinesis.getShardIterator)
        .map { i =>
          val iterator = i.getShardIterator
          val request = new GetRecordsRequest().withShardIterator(iterator)
          iterator -> kinesis.getRecordsAsync(request)
        }.toMap.seq
    }.onComplete(getAsyncCallback[Try[Map[String, JavaFuture[GetRecordsResult]]]] { iterators =>
      val unsafeIterators = iterators.get // trigger an exception if we could not bootstrap
      unsafeIterators.foreach(buffer += _)
    }.invoke(_))

    setHandler(outlet, new OutHandler {
      override def onPull() =
        tryToEmitRecords()
    })

    override def onTimer(timerKey: Any) =
      tryToEmitRecords()

    override def beforePreStart() =
      schedulePeriodically("kinesis", 100.millis)

    def tryToEmitRecords() = {
      buffer.filter(_._2.isDone).foreach { case (iterator, future) =>
        buffer.remove(iterator)
        val result = Try(future.get)
        val newIterator = result.toOption.fold(iterator)(_.getNextShardIterator)
        val newFuture = newIterator -> kinesis.getRecordsAsync(new GetRecordsRequest().withShardIterator(newIterator))

        log.debug(s"Emitting ${result.toOption.map(_.getRecords.size).getOrElse(0)} records...")
        emitMultiple(outlet, result.toOption.toList.flatMap(_.getRecords.asScala.map(_.getData).toList), { () =>
          buffer += newFuture
          ()
        })
      }
    }
  }
}
