package com.timeout

import java.security.MessageDigest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition}
import akka.stream.stage.GraphStageLogic.{EagerTerminateOutput, TotallyIgnorantInput}
import akka.stream.stage._
import akka.stream._
import com.amazonaws.services.kinesis.AmazonKinesisAsync
import com.amazonaws.services.kinesis.model._
import com.timeout.ToPutRecordsRequest._

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object KinesisGraphStage {

  private [timeout] val batchSize = 500

  /**
    * Given an A determine a partition key (0 to parallelism)
    * which is used to send it to a particular worker
    */
  private def partitionKey[A : ToPutRecordsRequest](parallelism: Int)(r: A): Int = {
    val hash = MessageDigest.getInstance("MD5").digest(r.toRequestEntry.getPartitionKey.getBytes)
    BigInt(hash).mod(parallelism).intValue
  }

  /**
    * Creates a Flow[A, A] for any A that can become a PutRecordsRequest.
    * The number of concurrent workers is determined by the parallelism,
    * and records will be distributed amongst the workers by their partition key
    */
  def apply[A : ToPutRecordsRequest](
    kinesis: AmazonKinesisAsync,
    streamName: String,
    parallelism: Int
  )(
    implicit
    as: ActorSystem,
    ec: ExecutionContext
  ): Flow[A, A, NotUsed] = Flow.fromGraph(
    GraphDSL.create[FlowShape[A, A]]() { implicit b =>
      import GraphDSL.Implicits._
      val split = b.add(Partition[A](parallelism, partitionKey(parallelism)))
      val merge = b.add(Merge[A](parallelism))
      (0 until parallelism).foreach { shard =>
        val stage = new KinesisGraphStage[A](kinesis, streamName, shard)
        val buffer = Flow[A].buffer(batchSize, OverflowStrategy.backpressure)
        split.out(shard) ~> buffer ~> stage ~> merge.in(shard)
      }
      FlowShape(split.in, merge.out)
    }
  )
}

private[timeout] class KinesisGraphStage[A : ToPutRecordsRequest](
  kinesis: AmazonKinesisAsync,
  streamName: String,
  worker: Int
)(
  implicit
  as: ActorSystem,
  ec: ExecutionContext
) extends GraphStage[FlowShape[A, A]] {

  private val in = Inlet[A]("PutRecordsRequestEntry")
  private val out = Outlet[A]("PutRecordsResultEntry")
  override def shape: FlowShape[A, A] = FlowShape(in, out)

  override def createLogic(a: Attributes) = new GraphStageLogic(shape) with LoggingAwsStage {
    setHandler(in, TotallyIgnorantInput)  // upstream finishing is handled in readThenPush
    setHandler(out, EagerTerminateOutput) // but if downstream stops we might as well too

    /**
      * Given a list of results from kinesis and their corresponding records,
      * emit all the records which we successfully pushed and retry those that failed
      */
    private def emitThenRead(req: Seq[A], resp: Seq[PutRecordsResultEntry]): Unit = {
      val (failures, successes) = req.zip(resp).partition { case (_, r) => r.getErrorCode != null }
      val (toResend, toEmit) = (failures.map(_._1).toList, successes.map(_._1).toList)
      val wait = (failures.length.toFloat / (failures.length + successes.length)).seconds
      log.info(s"$worker: ${toEmit.length} successes, ${toResend.length} failures, going again in $wait")
      emitMultiple(out, toEmit, () => waitThen(wait)(readThenPush(toResend)))
    }

    /**
      * Simply map an A to a PutRecordsRequest
      */
    private def createRequest(records: Seq[A]): PutRecordsRequest = {
      val entries = records.map(_.toRequestEntry).asJava
      new PutRecordsRequest().withRecords(entries).withStreamName(streamName)
    }

    /**
      * Given a bunch of records, push them to kinesis
      * and when finished call readThenPush to repeat the process
      */
    private def pushToKinesis(records: Seq[A]): Unit =
      run(createRequest(records))(kinesis.putRecordsAsync) {
        case Success(result) =>
          emitThenRead(records, result.getRecords.asScala)
        case Failure(e) =>
          log.error(e.getMessage)
          readThenPush(failures = records)
      }

    /**
      * Wait until we have 100 records, then try to push them to Kinesis.
      * Whether they succeed or fail, this method will then be called again,
      * setting up a readThenPush -> pushToKinesis -> emitThenRead -> readThenPush loop
      */
    private def readThenPush(failures: Seq[A] = List.empty): Unit = {
      if (!isClosed(in)) {
       readN(in, KinesisGraphStage.batchSize - failures.length)(
        r =>
          getAsyncCallback[Unit] { _ =>
            pushToKinesis(failures ++ r)
          }.invoke(()),
        r => getAsyncCallback[Unit] { _ =>
            if ((r ++ failures).isEmpty) {
              completeStage()
            } else {
              pushToKinesis(failures ++ r)
            }
          }.invoke(())
        )
      } else {
        completeStage() // todo what if we have failures
      }

    }


    /**
      * Before we start we need to kick off the read -> push loops mentioned above.
      * We kick off the same number of loops as there are shards in the stream
      */
    override def preStart() = {
      log.info(s"Pushing to $streamName")
      readThenPush()
    }
  }
}
