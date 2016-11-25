package com.timeout

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.amazonaws.services.kinesis.model.Record

object KinesisSource {
  def apply(): Source[Record, NotUsed] = ???
}
