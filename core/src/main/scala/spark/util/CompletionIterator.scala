package spark.util

import spark.InterruptibleIterator

/**
 * Wrapper around an iterator which calls a completion method after it successfully iterates through all the elements
 */
abstract class CompletionIterator[+A, +I <: Iterator[A]](sub: I) extends InterruptibleIterator[A]{
  override def next = sub.next
  override def hasNext = {
    val r = super.hasNext && sub.hasNext
    if (!r) {
      completion
    }
    r
  }

  def completion()
}

object CompletionIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: => Unit) : CompletionIterator[A,I] = {
    new CompletionIterator[A,I](sub) {
      def completion() = completionFunction
    }
  }
}