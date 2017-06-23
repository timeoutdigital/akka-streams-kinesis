package com.timeout

import akka.actor.ActorSystem
import akka.stream.stage.{GraphStageLogic, StageLogging}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

trait LoggingAwsStage extends StageLogging { self: GraphStageLogic =>

  /**
    * Adapt Amazon's 2 argument AsyncHandler based functions to execute a block on completion,
    * using Akka Streams' threadsafe getAsyncCallback function
    *
    * In most case the request argument is the same as the request the AsyncHandler gives you
    * but describeStreamAsync lets you pass in a string stream name, so we need a different type
    */
  protected def run[A, Req <: AmazonWebServiceRequest, Resp](
    requestArgument: A
  )(
    amazonAsyncFunction: (A, AsyncHandler[Req, Resp]) => Any
  )(
    whenDone: Try[Resp] => Unit
  ) = {
    val callback = getAsyncCallback[Try[Resp]](whenDone)
    val handler = new AsyncHandler[Req, Resp] {
      override def onError(exception: Exception) = callback.invoke(Failure(exception))
      override def onSuccess(request: Req, result: Resp) = callback.invoke(Success(result))
    }
    amazonAsyncFunction(requestArgument, handler)
  }

  protected def waitThen(
    duration: FiniteDuration
  )(
    block: => Unit
  )(
    implicit
    as: ActorSystem,
    ec: ExecutionContext
  ): Unit =
    as.scheduler.scheduleOnce(duration)(getAsyncCallback[Unit](_ => block).invoke(()))
}
