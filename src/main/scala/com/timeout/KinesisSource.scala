package com.timeout

import java.nio.ByteBuffer
import java.time.ZonedDateTime
import java.util.Date
import java.util.concurrent.{Future => JavaFuture}

import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, OutHandler, StageLogging, TimerGraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.{GetRecordsRequest, GetRecordsResult, GetShardIteratorRequest}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

object KinesisSource {

  def apply(stream: String, since: ZonedDateTime)(implicit ec: ExecutionContext) =
    Source.fromGraph(new KinesisSource(stream, since))

  private[timeout] lazy val kinesis: AmazonKinesisAsyncClient = new AmazonKinesisAsyncClient()
    .withRegion(Regions.EU_WEST_1) //todo why isn't this picked up from credentials
}

/**
  * A source for kinesis records
  * For the stream we maintain a map of current iterator => Future[GetRecordsResponse]
  * and we poll the map every 100ms to send more requests for every completed future
  */
private[timeout] class KinesisSource(stream: String, since: ZonedDateTime)(implicit val e: ExecutionContext)
  extends GraphStage[SourceShape[ByteBuffer]] {
  import KinesisSource.kinesis

  val outlet = Outlet[ByteBuffer]("Kinesis Records")
  override def shape = SourceShape[ByteBuffer](outlet)

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) with StageLogging {

    // stores futures from kinesis get records requests
    val buffer = mutable.Map.empty[String, JavaFuture[GetRecordsResult]]

    Future {
      kinesis.describeStream(stream).getStreamDescription.getShards.asScala.par.map { shard =>
        val req = new GetShardIteratorRequest()
          .withShardIteratorType("AT_TIMESTAMP")
          .withTimestamp(Date.from(since.toInstant))
          .withShardId(shard.getShardId)
          .withStreamName(stream)
        val it = kinesis.getShardIterator(req).getShardIterator
        it -> kinesis.getRecordsAsync(new GetRecordsRequest().withShardIterator(it))
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
        emitMultiple(outlet, result.toOption.toList.flatMap(_.getRecords.asScala.map(_.getData).toList))
        buffer += newIterator -> kinesis.getRecordsAsync(new GetRecordsRequest().withShardIterator(newIterator))
      }
    }
  }
}
