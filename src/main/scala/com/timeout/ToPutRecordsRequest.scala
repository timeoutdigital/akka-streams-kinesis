package com.timeout

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

/**
  * This type class should be implemented to allow pushing to kinesis,
  * it enables the graph stage to pass through the data it pushed unmodified on success
  */
trait ToPutRecordsRequest[A] {
  def toRequestEntry(a: A): PutRecordsRequestEntry
}

object ToPutRecordsRequest {

  // syntax sugar for creating new instances of ToPutRecordsRequest
  def instance[A](f: A => PutRecordsRequestEntry) = new ToPutRecordsRequest[A] {
    override def toRequestEntry(a: A) = f(a)
  }

  // syntax sugar for calling instances of ToPutRecordsRequest
  implicit class ToPutRecordsRequestSyntax[A : ToPutRecordsRequest](a: A) {
    def toRequestEntry = implicitly[ToPutRecordsRequest[A]].toRequestEntry(a)
  }
}
