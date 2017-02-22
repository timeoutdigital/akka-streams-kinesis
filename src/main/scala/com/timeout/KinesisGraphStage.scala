package com.timeout

import java.util.concurrent.Executors

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._
import com.timeout.KinesisGraphStage.FetchRecords
import org.slf4j.LoggerFactory
import ToPutRecordsRequest._
import scala.collection.convert.{DecorateAsJava, DecorateAsScala}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object KinesisGraphStage {
  type FetchRecords = PutRecordsRequest => PutRecordsResult
  val ProvisionedThroughputExceededExceptionCode = "ProvisionedThroughputExceededException"

  def withClient[A : ToPutRecordsRequest](client: AmazonKinesisClient, streamName: String): Flow[A, Either[PutRecordsResultEntry, A], NotUsed] =
    Flow.fromGraph(new KinesisGraphStage[A](client.putRecords, streamName))
}

/**
  * Asynchronous graph stage for publishing to kinesis
  * http://doc.akka.io/docs/akka/2.4.12/scala/stream/stream-customize.html
  * This graph stage maintains a buffer of items to push to kinesis and flushes it when full
  * The trick is that it then puts any failed items back into the buffer
  */
class KinesisGraphStage[A : ToPutRecordsRequest](fetch: FetchRecords, streamName: String)
  extends GraphStage[FlowShape[A, Either[PutRecordsResultEntry, A]]]
  with DecorateAsScala
  with DecorateAsJava {
  private val logger = LoggerFactory.getLogger(getClass)
  private val in = Inlet[A]("PutRecordsRequestEntry")
  private val out = Outlet[Either[PutRecordsResultEntry, A]]("PutRecordsResultEntry")
  override def shape: FlowShape[A, Either[PutRecordsResultEntry, A]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val sendingThreshold = 250
    private val maxBufferSize = 500 // hard limit imposed by AWS
    private val kinesisBackoffTime = 800
    private var recordsInFlight: Int = 0
    private val inputBuffer = mutable.Queue.empty[A]
    private implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)

    /**
      * Respond to any kind of stream event
      * Whatever happens we want to add any new entries to the buffer
      * then run through a series of checks:
      *
      *  - is the buffer empty and the producer closed? We're done here
      *  - is the buffer full? Then lets dispatch the kinesis worker to clear it and lets not ask for more
      *  - Is the producer closed? Then even if the buffer isn't full lets clear it
      *  - is the buffer not full? Lets ask for something new
      *
      *  Note that we /either/ ask for new data or we push to kinesis - never both
      *  We will therefore backpressure while waiting for kinesis to finish.
      *  If we did everything synchronously we would block rather than backpressure
      *  which would prevent us filling up any upstream buffers
      */
    private def streamStateChanged(newRecords: List[A] = List.empty) = {
      inputBuffer.enqueue(newRecords: _*)

      if (inputBuffer.isEmpty && isClosed(in) && recordsInFlight < 1) {
        completeStage()
      } else if (recordsInFlight < 1 && (inputBuffer.length >= sendingThreshold || isClosed(in))) {
        pushToKinesis()
      }

      if (!hasBeenPulled(in) && !isClosed(in) && (inputBuffer.length + recordsInFlight) < maxBufferSize) {
        pull(in)
      }
    }

    /**
      * Take everything in inputBuffer and push it to kinesis with kinesisWorkerThread
      * If we get throughput exceeded issues we will block the kinesis worker for a 800ms
      * to give it a chance to recover, then put the failed items back onto the buffer
      */
    def pushToKinesis(): Unit = {
      val dataToPush = inputBuffer.toList
      inputBuffer.clear // surely there must be a collection with a removeN
      recordsInFlight = dataToPush.size

      Future {
        // everything in here happens in an async worker thread
        val request = new PutRecordsRequest()
          .withRecords(dataToPush.map(_.toRequestEntry).asJava)
          .withStreamName(streamName)

        def incrementalBackoff(err: Throwable, n: Int): Unit = {
          val waitSec = Math.pow(2, n).toInt
          logger.debug(s"Error while trying to push to Kinesis: $err.\nBacking off for $waitSec seconds")
          Thread.sleep(waitSec * 1000)
        }

        val results = withRetries(fetch(request), onError = incrementalBackoff).getRecords.asScala
        /*
         * We rate limit ourselves here in the worker thread
         * Blocking in getAsyncCallback would block the entire stream
         * While here the stream can continue to fill any buffers preceeding us
         */
        val throttled = results.count(_.getErrorCode == KinesisGraphStage.ProvisionedThroughputExceededExceptionCode)
        if (throttled > 0) {
          Thread.sleep(kinesisBackoffTime)
        }

        results.zip(dataToPush).toList
      }.foreach(getAsyncCallback[List[(PutRecordsResultEntry, A)]] { resultsAndRequests =>
        recordsInFlight = 0

        // in here we're back in an akka streams managed thread
        val (throughputErrors, otherResults) = resultsAndRequests.partition { case (err, _) =>
          err.getErrorCode == KinesisGraphStage.ProvisionedThroughputExceededExceptionCode
        }

        val (errors, successes) = otherResults.partition { case(result, _) => result.getErrorCode != null }
        val results = errors.map(e => Left(e._1)) ++ successes.map(s => Right(s._2))
        emitMultiple(out, results)

        // put any failures back into the buffer
        streamStateChanged(throughputErrors.map(_._2))
      }.invoke(_))
    }

    override def beforePreStart(): Unit =
      streamStateChanged()

    setHandler(in, new InHandler {

      override def onUpstreamFinish(): Unit = {
        streamStateChanged()
      }


      override def onPush(): Unit = {
        val entry = grab(in)
        streamStateChanged(List(entry))
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = streamStateChanged(List.empty)
    })
  }
}
