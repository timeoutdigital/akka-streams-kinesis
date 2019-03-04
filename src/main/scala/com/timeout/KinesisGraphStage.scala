package com.timeout

import java.util.concurrent.Executors

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model._
import com.timeout.KinesisGraphStage.PutRecords
import com.timeout.ToPutRecordsRequest._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object KinesisGraphStage {

  type PutRecords = PutRecordsRequest => PutRecordsResult
  val ProvisionedThroughputExceededExceptionCode = "ProvisionedThroughputExceededException"

  private[KinesisGraphStage] val maxBufferSize = 500 // hard limit imposed by AWS
  private[KinesisGraphStage] val defaultSendingThreshold = 250 // number of entires
  private[KinesisGraphStage] val kinesisBackoffTime = 800
  private[KinesisGraphStage] val maxRequestSize = 5000000 // in bytes; limit imposed by AWS

  def withClient[A : ToPutRecordsRequest](client: AmazonKinesis,
                                          streamName: String,
                                          sendingThreshold: Int = defaultSendingThreshold
                                         ): Flow[A, Either[PutRecordsResultEntry, A], NotUsed] =
    Flow.fromGraph(new KinesisGraphStage[A](client.putRecords, streamName, sendingThreshold))
}

/**
  * Asynchronous graph stage for publishing to kinesis
  * http://doc.akka.io/docs/akka/2.4.12/scala/stream/stream-customize.html
  * This graph stage maintains a buffer of items to push to kinesis and flushes it when full
  * The trick is that it then puts any failed items back into the buffer
  */
class KinesisGraphStage[A : ToPutRecordsRequest](putRecords: PutRecords, streamName: String, sendingThreshold: Int)
  extends GraphStage[FlowShape[A, Either[PutRecordsResultEntry, A]]] {

  private val in = Inlet[A]("PutRecordsRequestEntry")
  private val out = Outlet[Either[PutRecordsResultEntry, A]]("PutRecordsResultEntry")
  override def shape: FlowShape[A, Either[PutRecordsResultEntry, A]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with StageLogging {

    import KinesisGraphStage._
    private var recordsInFlight: Int = 0
    private val inputBuffer = mutable.Queue.empty[A]
    private implicit val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)

    /**
      * Respond to any kind of stream event
      */
    private def streamStateChanged(newRecords: List[A] = List.empty) = {
      inputBuffer.enqueue(newRecords: _*)

      // is the buffer empty and the producer closed? We're done here
      if (inputBuffer.isEmpty && isClosed(in) && recordsInFlight < 1) {
        completeStage()

      // is the buffer full? Then lets dispatch the kinesis worker to clear it
      // otherwise is the producer closed? Then even if the buffer isn't full lets clear it
      } else if (recordsInFlight < 1 && (inputBuffer.length >= sendingThreshold || isClosed(in))) {
        pushToKinesis()
      }

      // is the buffer not full? Lets ask for something new
      if (!hasBeenPulled(in) && !isClosed(in) && (inputBuffer.length + recordsInFlight) < maxBufferSize) {
        pull(in)
      }
    }

    /**
      * Split
      */
    private def splitBySize(entries: List[PutRecordsRequestEntry]): List[List[PutRecordsRequestEntry]] = {
      val zero = 0 -> List.empty[PutRecordsRequestEntry] -> List.empty[List[PutRecordsRequestEntry]]
      entries.foldLeft(zero) { case (((total, newList), lists), entry) =>
        val size = entry.getData.limit() // an approximation, as in the request it will have different size
        if (total + size > maxRequestSize)
          size -> List(entry) -> (lists :+ newList)
        else
          (total + size) -> (newList :+ entry) -> lists
      } match {
        case ((_, newList), lists) =>
          if (newList.nonEmpty)
            lists :+ newList
          else
            lists
      }
    }

    /**
      * Take everything in inputBuffer and push it to kinesis with kinesisWorkerThread
      * If we get throughput exceeded issues we will block the kinesis worker for a 800ms
      * to give it a chance to recover, then put the failed items back onto the buffer
      */
    def pushToKinesis(): Unit = {
      val dataToPush = inputBuffer.toList
      inputBuffer.clear
      recordsInFlight = dataToPush.size

      Future {
        val maxSize = 0

        val grouppedData: List[List[PutRecordsRequestEntry]] = splitBySize(dataToPush.map(_.toRequestEntry))

        def incrementalBackoff(err: Throwable, n: Int): Unit = {
          val waitSec = Math.pow(2, Math.max(n, 12)).toInt // Don't not wait longer than 4096 seconds between two next tries
          log.error(s"Error while trying to push to Kinesis: $err.\nBacking off for $waitSec seconds (try #$n)")
          Thread.sleep(waitSec * 1000)
        }

        val resultsByRequest = for {
          dataToPush <- grouppedData
          request = new PutRecordsRequest()
            .withRecords(dataToPush.asJava)
            .withStreamName(streamName)
        } yield withRetries(putRecords(request), onError = incrementalBackoff).getRecords.asScala.toList
        val results = resultsByRequest.flatten

        /*
         * We rate limit ourselves here in the worker thread
         * Blocking in getAsyncCallback would block the entire stream
         * While here the stream can continue to fill any buffers preceding us
         */
        val throttled = results.count(_.getErrorCode == ProvisionedThroughputExceededExceptionCode)
        if (throttled > 0) {
          Thread.sleep(kinesisBackoffTime)
        }

        results.zip(dataToPush).toList
      }.foreach(getAsyncCallback[List[(PutRecordsResultEntry, A)]] { resultsAndRequests =>
        recordsInFlight = 0

        // in here we're back in an akka streams managed thread
        val (throughputErrors, otherResults) = resultsAndRequests.partition { case (err, _) =>
          err.getErrorCode == ProvisionedThroughputExceededExceptionCode
        }

        log.debug(s"pushed ${otherResults.size}/${resultsAndRequests.size} records to kinesis")
        val (errors, successes) = otherResults.partition { case (result, _) => result.getErrorCode != null }
        val results = errors.map(e => Left(e._1)) ++ successes.map(s => Right(s._2))
        emitMultiple(out, results)

        // put any failures back into the buffer
        streamStateChanged(throughputErrors.map(_._2))
      }.invoke(_))
    }

    override def beforePreStart() =
      streamStateChanged()

    setHandler(in, new InHandler {
      override def onUpstreamFinish() =
        streamStateChanged()
      override def onPush() =
        streamStateChanged(List(grab(in)))
    })

    setHandler(out, new OutHandler {
      override def onPull() =
        streamStateChanged(List.empty)
    })
  }
}
