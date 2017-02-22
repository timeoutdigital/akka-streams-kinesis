package com

import scala.annotation.tailrec

package object timeout {
  @tailrec
  // 2 ^ 6 = 64
  def withRetries[T](f: => T, tries: Int = 7, onError: (Throwable, Int) => Unit): T =
    try { f } catch { case err: Throwable =>
      if (tries < 1)
        throw err
      else {
        onError(err, tries)
        withRetries(f, tries - 1, onError)
      }
    }
}
