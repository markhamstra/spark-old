package spark

import java.util.concurrent.{Callable, CountDownLatch, Future, FutureTask, TimeUnit}
import scala.concurrent.ops.spawn
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import spark.scheduler.Task

class CancellationSuite extends FunSuite with BeforeAndAfter {

  test("Cancel Task") {
    val latch = new CountDownLatch(2)
    val startLatch = new CountDownLatch(1)
    val task = new Task[Int](0) {
      override def run(attemptId: Long) = {
        try {
          startLatch.countDown()
          super.run(attemptId)
        } catch {
        	//check if interrupted exception is thrown
        	case e: InterruptedException => latch.countDown()
        }
        0
      }
      override def runInterruptibly(attemptId: Long) = {
        //check if interrupt is propagated
        while(!Thread.currentThread().isInterrupted()) {}
        latch.countDown()
        0
      }
    }
    spawn {
      task.run(0)
    }
    startLatch.await()
    Thread.sleep(100)
    task.kill
    val v = latch.await(5,TimeUnit.SECONDS)
    assert(latch.getCount() == 0 && v)
  }
  test("handle interrupt during iteration") {
    val latch = new CountDownLatch(1)
    val innerLatch = new CountDownLatch(1)
    val iter = new Iterator[Int] with InterruptibleIterator[Int] {
      override def hasNext(): Boolean = super.hasNext && true
      override def next(): Int = 0
    }
    val f = new FutureTask(new Callable[Int]{
      override def call(): Int = {
        var count=0
        latch.countDown()
        try{
          while (iter.hasNext) {
            count += iter.next
          }
        } finally {
          innerLatch.countDown()
        }
        count
      }
    })
    spawn {
      latch.await()
      Thread.sleep(100)
      f.cancel(true)
    }
    f.run()
    try {
      f.get()
    } catch {
      case e: InterruptedException => {
        innerLatch.await(1, TimeUnit.SECONDS)
      }
    } finally {
      assert(innerLatch.getCount() == 0)
    }
  }

}