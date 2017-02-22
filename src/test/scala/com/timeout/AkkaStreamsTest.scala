package com.timeout

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

trait AkkaStreamsTest extends FreeSpec with BeforeAndAfterAll {

  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()

  override def afterAll() = {
    super.afterAll()
    as.terminate()
  }

}
