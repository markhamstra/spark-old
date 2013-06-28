package spark

trait InterruptibleIterator[+T] extends  Iterator[T]{

  override def hasNext(): Boolean = {
    if (!Thread.currentThread().isInterrupted()) {
      true
    } else {
      throw new InterruptedException ("Thread interrupted during RDD iteration")
    }
  }

}

class InterruptibleIteratorDecorator[T](delegate: Iterator[T])
	extends AnyRef with InterruptibleIterator[T] {
  
  override def hasNext(): Boolean = {
    super.hasNext
    delegate.hasNext
  }
  
  override def next(): T = {
    delegate.next()
  }
}