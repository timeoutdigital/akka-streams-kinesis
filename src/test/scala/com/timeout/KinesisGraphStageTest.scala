package com.timeout
import java.nio.ByteBuffer

import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResult, PutRecordsResultEntry}
import com.timeout.KinesisGraphStage._
import org.scalatest.Matchers
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.JavaConverters._

class KinesisGraphStageTest extends AkkaStreamsTest with Matchers with PatienceConfiguration with ScalaFutures {
  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  def requestEntry(id: Int) =
    new PutRecordsRequestEntry().withData(ByteBuffer.wrap(s"num: $id".getBytes))

  def resultEntry =
    new PutRecordsResultEntry()

  def result(records: Seq[PutRecordsResultEntry]): PutRecordsResult = new PutRecordsResult()
    .withFailedRecordCount(records.count(_.getErrorCode != null))
    .withRecords(records.asJava)

  val successClient: KinesisGraphStage.FetchRecords = r =>
    result(r.getRecords.asScala.map(_ => resultEntry).toList)

  val failingClient: KinesisGraphStage.FetchRecords = r =>
    result((1 to r.getRecords.size).map(_ => resultEntry.withErrorMessage("Failure").withErrorCode("F")))

  def kinesis(client: KinesisGraphStage.FetchRecords) =
    new KinesisGraphStage(client, "test")


  "Kinesis graph stage" - {

    "Emit the right number of results when everything works" in {
      val output = Source(0 to 4).map(requestEntry).via(kinesis(successClient)).runWith(Sink.seq)
      whenReady(output)(_ shouldEqual (0 to 4).map(_ => resultEntry))
    }

    "Emit the right number of failures when kinesis fails for reasons other than throughput" in {
      val output = Source.single(requestEntry(id = 0)).via(kinesis(failingClient)).runWith(Sink.seq)
      whenReady(output)(_ shouldEqual List(resultEntry.withErrorMessage("Failure").withErrorCode("F")))
    }

    "Retry sending if kinesis fails due to throughput exceptions" in {

      val throttledClient: KinesisGraphStage.FetchRecords = r =>
        if (r.getRecords.size == 3) {
          result(Seq(resultEntry, resultEntry, resultEntry
            .withErrorCode(ProvisionedThroughputExceededExceptionCode)
            .withErrorMessage("Failure")
          ))
        } else {
          successClient(r)
        }

      val output = Source(0 to 2).map(requestEntry)
        .via(kinesis(throttledClient)).runWith(Sink.seq)

      whenReady(output) { res =>
        res shouldEqual (0 to 2).map(_ => resultEntry)
      }
    }
  }
}
