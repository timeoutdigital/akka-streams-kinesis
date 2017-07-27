package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime}
import java.util.Date

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.EagerTerminateOutput
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.AmazonServiceException.ErrorType
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.timeout.KinesisSource.IteratorType

import scala.collection.JavaConverters._
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

  private [timeout] case class ShardId(value: String) extends AnyVal

  /**
    * A wrapper for a Kinesis shard iterator, that knows how to reissue itself
    * should it expire due to the 5 minute iterator validity cutoff
    */
  private [timeout] case class ShardIterator(
    shard: ShardId,
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
    ShardIterator(s.shard, g.getNextShardIterator, reissue)
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
    clock: Clock = Clock.systemUTC
  ): Source[ByteBuffer, NotUsed] =
    Source.fromGraph(new KinesisSource(kinesis, stream, iterator))

  /**
    * Construct shard iterator requests
    * based on a stream description
    */
  private[timeout] def shardIteratorRequests(
    iteratorType: IteratorType,
    shards: List[ShardId],
    stream: String
  )(
    implicit
    clock: Clock
  ): List[(ShardId, GetShardIteratorRequest)] =
    shards.map { shard =>
      val request = new GetShardIteratorRequest()
        .withStreamName(stream)
        .withShardId(shard.value)

      val withIteratorType = iteratorType match {
        case IteratorType.AtTimestamp(since) =>
          val now = ZonedDateTime.now(clock)
          val readFrom = if (since.isBefore(now)) since else now
          request.withShardIteratorType("AT_TIMESTAMP").withTimestamp(Date.from(readFrom.toInstant))
        case IteratorType.TrimHorizon =>
          request.withShardIteratorType("TRIM_HORIZON")
        case IteratorType.Latest =>
          request.withShardIteratorType("LATEST")
      }
      shard -> withIteratorType
    }

  /**
    * Given a Kinesis Stream, and an optional parent shard to find children of,
    * produce a list of either top level shards (if no parent) or child shards
    */
  private [timeout] def findOldestPossibleShards(
    stream: DescribeStreamResult
  ) = {
    val awsShards = stream.getStreamDescription.getShards.asScala.toList
    val awsShardIds = awsShards.map(_.getShardId).toSet

    awsShards
      .map(s => if (!awsShardIds.contains(s.getParentShardId)) s.withParentShardId(null) else s)
      .filter(s => s.getParentShardId == null)
      .map(s => ShardId(s.getShardId))
  }

  /**
    * If we've asked to read LATEST we don't want to find the oldest shards,
    * we want to find the newest shards, so exclude any shards with children
    */
  private [timeout] def findNewestPossibleShards(
    stream: DescribeStreamResult
  ) = {
    val awsShards = stream.getStreamDescription.getShards.asScala.toList
    val parentShardIds: Set[String] = awsShards.flatMap { s =>
      Option(s.getParentShardId) ++ Option(s.getAdjacentParentShardId)
    }.toSet
    awsShards
      .filterNot(s => parentShardIds.contains(s.getShardId))
      .map(s => ShardId(s.getShardId))
  }

  /**
    * Find shards which are descendants of the given shard ID
    * We ignore adjacentParentId so in the case of shards being merged we only get one child
    */
  private [timeout] def findChildShards(
    stream: DescribeStreamResult,
    parent: ShardId
  ): List[ShardId] = {
    stream.getStreamDescription.getShards.asScala.toList.collect {
      case s if s.getParentShardId == parent.value => ShardId(s.getShardId)
    }
  }

  /**
    * when we're reading LATEST and need to begin reading from a new shard we want to TRIM_HORIZON it
    * just to ensure we don't miss any records added during our describe stream call
    */
  private [timeout] def iteratorForReshard(iterator: IteratorType) = iterator match {
    case IteratorType.Latest => IteratorType.TrimHorizon
    case it => it
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
  clock: Clock
) extends GraphStage[SourceShape[ByteBuffer]] {

  import KinesisSource._
  private val outlet = Outlet[ByteBuffer]("Kinesis Records")
  override def shape: SourceShape[ByteBuffer] = SourceShape[ByteBuffer](outlet)

  override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) with StageLogging {
    setHandler(outlet, EagerTerminateOutput)

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
        override def onError(exception: Exception): Unit = callback.invoke(Failure(exception))
        override def onSuccess(request: Req, result: Resp): Unit = callback.invoke(Success(result))
      }
      amazonAsyncFunction(requestArgument, handler)
    }

    /**
      * Set up the asynchronous mutual recursion getRecords -> emitThenGetRecords / reissueThenGetRecords loop
      * for a particular list of shards and a particular shard iterator type
      */
    private def beginReadingFromShards(shards: List[ShardId], iteratorType: IteratorType): Unit = {
      shardIteratorRequests(iteratorType, shards, streamName).foreach { case (shard, request) =>
        run(request)(kinesis.getShardIteratorAsync) { iteratorResult =>
          Option(iteratorResult.get.getShardIterator).fold {
            log.warning(s"$streamName: No iterator for $shard")
            handleReshard(shard)
          } { it =>
            log.debug(s"$streamName: Beginning to read from ${shard.value} at $iteratorType")
            getRecords(ShardIterator(shard, it, request))
          }
        }
      }
    }

    /**
      * Handle a shard being closed,
      * i.e. find its children and read from them
      */
    private def handleReshard(closedShard: ShardId): Unit = {
      run(streamName)(kinesis.describeStreamAsync) { stream =>
        val shards: List[ShardId] = findChildShards(stream.get, closedShard)
        val newIterator = iteratorForReshard(iterator)
        beginReadingFromShards(shards, newIterator)
      }
    }

    /**
      * bootstrap everything by getting initial shard iterators
      * Any errors here are essentially unrecoverable so we explode, hence the .gets
      */
    override def preStart(): Unit =
      run(streamName)(kinesis.describeStreamAsync) { stream =>
        val shards = iterator match {
          case IteratorType.Latest => findNewestPossibleShards(stream.get)
          case _ => findOldestPossibleShards(stream.get)
        }
        beginReadingFromShards(shards, iterator)
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
        Option(result.getNextShardIterator).fold {
          log.debug(s"${currentIterator.shard.value} has been closed")
          handleReshard(currentIterator.shard)
        } { _ =>
          getRecords(nextIterator(currentIterator, result))
        }
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
     case Failure(_: ProvisionedThroughputExceededException) =>
        getRecords(iterator)
     case Failure(error: AmazonKinesisException) if error.getErrorType == ErrorType.Client =>
        throw error // any client errors are bugs in this library
      case Failure(error) =>
        log.error(error.getMessage)
        reissueThenGetRecords(iterator)
    }
  }

}
