package com.timeout

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

trait AkkaStreamsTest extends AnyFreeSpec with BeforeAndAfterAll {

  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher

  override def afterAll() = {
    super.afterAll()
    as.terminate()
  }
}
