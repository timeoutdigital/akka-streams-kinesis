package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime}
import java.util.Date

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.timeout.KinesisSource.IteratorType

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object KinesisSource {

  /**
    * Models the ShardIteratorType parameter passed to GetShardIterator
    * http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
    * We don't support sequence number based iterators yet as the source does not emit sequence numbers
    */
  sealed trait IteratorType

  object IteratorType {
    case class AtTimestamp(time: ZonedDateTime) extends IteratorType
    case object TrimHorizon extends IteratorType
    case object Latest extends IteratorType
  }

  /**
    * A wrapper for a Kinesis shard iterator, that knows how to reissue itself
    * should it expire due to the 5 minute iterator validity cutoff
    */
  private [timeout] case class ShardIterator(
    iterator: String,
    reissue: GetShardIteratorRequest
  )

  /**
    * Given a response from kinesis and the current shard iterator, prepare a new iterator.
    * The new iterator needs to have a reissue command that can reproduce it if it expires.
    * To do this we either use an AFTER_SEQUENCE_NUMBER request if we have records, or
    * just use the last reissue if we don't (which I can't see ever happening)
    */
  private [timeout] def nextIterator(
    s: ShardIterator,
    g: GetRecordsResult
  )(implicit c: Clock): ShardIterator = {
    val reissue = g.getRecords.asScala.lastOption.fold(s.reissue) { lastRecord =>
      s.reissue
        .withShardIteratorType("AFTER_SEQUENCE_NUMBER")
        .withStartingSequenceNumber(lastRecord.getSequenceNumber)
    }
    ShardIterator(g.getNextShardIterator, reissue)
  }

  /**
    * This creates a source that reads records from AWS Kinesis.
    * It is serialisation format agnostic so emits a stream of ByteBuffers
    */
  def apply(
    kinesis: AmazonKinesisAsync,
    stream: String,
    iterator: IteratorType
  )(
    implicit
    ec: ExecutionContext,
    clock: Clock = Clock.systemUTC
  ): Source[ByteBuffer, NotUsed] =
    Source.fromGraph(new KinesisSource(kinesis, stream, iterator))

  /**
    * Construct shard iterator requests
    * based on a stream description
    */
  private[timeout] def shardIteratorRequests(
    iteratorType: IteratorType,
    stream: StreamDescription
  )(
    implicit
    clock: Clock
  ): List[GetShardIteratorRequest] =
    stream.getShards.asScala.toList.map { shard =>
      val request = new GetShardIteratorRequest()
        .withStreamName(stream.getStreamName)
        .withShardId(shard.getShardId)

      iteratorType match {
        case IteratorType.AtTimestamp(since) =>
          val now = ZonedDateTime.now(clock)
          val readFrom = if (since.isBefore(now)) since else now
          request.withShardIteratorType("AT_TIMESTAMP").withTimestamp(Date.from(readFrom.toInstant))
        case IteratorType.TrimHorizon =>
          request.withShardIteratorType("TRIM_HORIZON")
        case IteratorType.Latest =>
          request.withShardIteratorType("LATEST")
      }
    }
}

/**
  * A source for kinesis records
  */
private[timeout] class KinesisSource(
  kinesis: AmazonKinesisAsync,
  streamName: String,
  iterator: IteratorType
)(
  implicit
  e: ExecutionContext,
  clock: Clock
) extends GraphStage[SourceShape[ByteBuffer]] {

  import KinesisSource._
  val outlet = Outlet[ByteBuffer]("Kinesis Records")
  override def shape = SourceShape[ByteBuffer](outlet)

  override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) with StageLogging {

    /**
      * Adapt Amazon's 2 argument AsyncHandler based functions to execute a block on completion,
      * using Akka Streams' threadsafe getAsyncCallback function
      *
      * In most case the request argument is the same as the request the AsyncHandler gives you
      * but describeStreamAsync lets you pass in a string stream name, so we need a different type
      */
    private def run[A, Req <: AmazonWebServiceRequest, Resp](
      requestArgument: A
    )(
      amazonAsyncFunction: (A, AsyncHandler[Req, Resp]) => Any
    )(
      whenDone: Try[Resp] => Unit
    ) = {
      val callback = getAsyncCallback[Try[Resp]](whenDone)
      val handler = new AsyncHandler[Req, Resp] {
        override def onError(exception: Exception) = callback.invoke(Failure(exception))
        override def onSuccess(request: Req, result: Resp) = callback.invoke(Success(result))
      }
      amazonAsyncFunction(requestArgument, handler)
    }

    /**
      * We don't want to respond to any pull events from downstream
      * as we're going to push records to them as quickly as we can
      */
    setHandler(outlet, new OutHandler {
      override def onPull() = Unit
    })

    /**
      * bootstrap everything by getting initial shard iterators
      * Any errors here are essentially unrecoverable so we explode, hence the .gets
      */
    run(streamName)(kinesis.describeStreamAsync) { stream =>
      shardIteratorRequests(iterator, stream.get.getStreamDescription).foreach { request =>
        run(request)(kinesis.getShardIteratorAsync) { iteratorResult =>
          getRecords(ShardIterator(iteratorResult.get.getShardIterator, request))
        }
      }
    }

    /**
      * Get records from Kinesis, then call handleResult
      * to deal with errors or emitting the results
      */
    //noinspection AccessorLikeMethodIsUnit
    private def getRecords(it: ShardIterator): Unit = {
      val request = new GetRecordsRequest().withShardIterator(it.iterator)
      run(request)(kinesis.getRecordsAsync)(handleResult(it))
    }

    /**
      * Given a result from getRecords, emit it
      * then call getRecords again when we're finished
      */
    private def emitThenGetRecords(currentIterator: ShardIterator, result: GetRecordsResult): Unit = {
      emitMultiple[ByteBuffer](outlet, result.getRecords.asScala.map(_.getData).toList, { () =>
        getRecords(nextIterator(currentIterator, result))
      })
    }

    /**
      * Given a shard iterator, reissue it
      * then call getRecords with the new iterator
      */
    private def reissueThenGetRecords(iterator: ShardIterator): Unit = {
      log.debug(s"$streamName - reissuing shard iterator")
      run(iterator.reissue)(kinesis.getShardIteratorAsync) { r =>
        getRecords(iterator.copy(iterator = r.get.getShardIterator))
      }
    }

    /**
      * Handle the results of a Kinesis GetRecords call by dispatching
      * to the above functions dependent on what happened.
      */
    private def handleResult(iterator: ShardIterator)(res: Try[GetRecordsResult]) = res match {
      case Success(recordsResult) =>
        emitThenGetRecords(iterator, recordsResult)
      case Failure(_: ExpiredIteratorException) =>
        reissueThenGetRecords(iterator)
      case Failure(error) =>
        log.error(error.getMessage)
        getRecords(iterator)
    }
  }
}
