package com.timeout

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

object KinesisSink {
  def withClient(amazonKinesisClient: AmazonKinesisClient, stream: String): Sink[PutRecordsRequestEntry, NotUsed] = ???
}
