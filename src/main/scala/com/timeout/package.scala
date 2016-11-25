package com

import scala.annotation.tailrec

package object timeout {
  @tailrec
  // 2 ^ 6 = 64
  def withRetries[T](f: => T, tries: Int = 7): T =
    try { f } catch { case err: Throwable =>
      if (tries < 1)
        throw err
      else
        println(err.getMessage)
        withRetries(f, tries - 1)
    }
}
