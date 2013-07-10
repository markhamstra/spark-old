package spark.util

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import scala.collection.mutable.Buffer
import scala.concurrent.ops.spawn
import java.util.NoSuchElementException
import java.util.concurrent.{Callable, CountDownLatch, Future, FutureTask, TimeUnit}

class NextIteratorSuite extends FunSuite with ShouldMatchers {
  test("one iteration") {
    val i = new StubIterator(Buffer(1))
    i.hasNext should be === true
    i.next should be === 1
    i.hasNext should be === false
    intercept[NoSuchElementException] { i.next() }
  }
  
  test("two iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.hasNext should be === true
    i.next should be === 1
    i.hasNext should be === true
    i.next should be === 2
    i.hasNext should be === false
    intercept[NoSuchElementException] { i.next() }
  }

  test("empty iteration") {
    val i = new StubIterator(Buffer())
    i.hasNext should be === false
    intercept[NoSuchElementException] { i.next() }
  }

  test("close is called once for empty iterations") {
    val i = new StubIterator(Buffer())
    i.hasNext should be === false
    i.hasNext should be === false
    i.closeCalled should be === 1
  }

  test("close is called once for non-empty iterations") {
    val i = new StubIterator(Buffer(1, 2))
    i.next should be === 1
    i.next should be === 2
    // close isn't called until we check for the next element
    i.closeCalled should be === 0
    i.hasNext should be === false
    i.closeCalled should be === 1
    i.hasNext should be === false
    i.closeCalled should be === 1
  }

  test("close is called upon interruption") {
    val latch = new CountDownLatch(1)
    val startLatch = new CountDownLatch(1)
    var closeCalled = 0

    val iter = new NextIterator[Int] {
    
      override def getNext() = {
        if (latch.getCount() == 0) {
          finished = true
          0
        }
        1
      }
  
      override def close() {
        latch.countDown()
        closeCalled += 1
      }
    }
     val f = new FutureTask(new Callable[Int]{
      override def call(): Int = {
        var count=0
        startLatch.countDown()
        while (iter.hasNext) {
          count += iter.next
        }
        count
      }
    })

    spawn {
      startLatch.await()
      Thread.sleep(100)
      f.cancel(true)
    }
    f.run()
    try {
      f.get()
    } catch {
      case e: InterruptedException => {
        latch.await(1, TimeUnit.SECONDS)
      }
    } finally {
      assert(latch.getCount() == 0 && closeCalled == 1)
    }
  }

  class StubIterator(ints: Buffer[Int])  extends NextIterator[Int] {
    var closeCalled = 0
    
    override def getNext() = {
      if (ints.size == 0) {
        finished = true
        0
      } else {
        ints.remove(0)
      }
    }

    override def close() {
      closeCalled += 1
    }
  }
}
