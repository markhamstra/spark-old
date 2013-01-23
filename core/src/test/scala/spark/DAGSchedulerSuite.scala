/*
* Copyright (c) Clear Story Data, Inc. All Rights Reserved.
*
* Please see the COPYRIGHT file in the root of this repository for more
* details.
*/
package spark

import org.scalatest.FunSuite
import scheduler.{DAGScheduler, TaskSchedulerListener, TaskSet, TaskScheduler}
import collection.mutable

class TaskSchedulerMock(f: (Int) => TaskEndReason ) extends TaskScheduler {
  // Listener object to pass upcalls into
  var listener: TaskSchedulerListener = null
  var taskCount = 0

  override def start(): Unit = {}

  // Disconnect from the cluster.
  override def stop(): Unit = {}

  // Submit a sequence of tasks to run.
  override def submitTasks(taskSet: TaskSet): Unit = {
    taskSet.tasks.foreach( task => {
      val m = new mutable.HashMap[Long, Any]()
      m.put(task.stageId, 1)
      taskCount += 1
      listener.taskEnded(task, f(taskCount), 1, m)
    })
  }

  // Set a listener for upcalls. This is guaranteed to be set before submitTasks is called.
  override def setListener(listener: TaskSchedulerListener) {
    this.listener = listener
  }

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  override def defaultParallelism(): Int = {
    2
  }
}

class DAGSchedulerSuite extends FunSuite {
  def assertDagSchedulerEmpty(dagScheduler: DAGScheduler) = {
    assert(dagScheduler.pendingTasks.isEmpty)
    assert(dagScheduler.activeJobs.isEmpty)
    assert(dagScheduler.failed.isEmpty)
    assert(dagScheduler.runIdToStageIds.isEmpty)
    assert(dagScheduler.idToStage.isEmpty)
    assert(dagScheduler.resultStageToJob.isEmpty)
    assert(dagScheduler.running.isEmpty)
    assert(dagScheduler.shuffleToMapStage.isEmpty)
    assert(dagScheduler.waiting.isEmpty)
  }

  test("oneGoodJob") {
    val sc = new SparkContext("local", "test")
    val dagScheduler = new DAGScheduler(new TaskSchedulerMock(count => Success))
    try {
      val rdd = new ParallelCollection(sc, 1.to(100).toSeq, 5, Map.empty)
      val func = (tc: TaskContext, iter: Iterator[Int]) => 1
      val callSite = Utils.getSparkCallSite

      val result = dagScheduler.runJob(rdd, func, 0 until rdd.splits.size, callSite, false)
      assertDagSchedulerEmpty(dagScheduler)
    } finally {
      dagScheduler.stop()
      sc.stop()
      // pause to let dagScheduler stop (separate thread)
      Thread.sleep(10)
    }
  }

  test("manyGoodJobs") {
    val sc = new SparkContext("local", "test")
    val dagScheduler = new DAGScheduler(new TaskSchedulerMock(count => Success))
    try {
      val rdd = new ParallelCollection(sc, 1.to(100).toSeq, 5, Map.empty)
      val func = (tc: TaskContext, iter: Iterator[Int]) => 1
      val callSite = Utils.getSparkCallSite

      1.to(100).foreach( v => {
        val result = dagScheduler.runJob(rdd, func, 0 until rdd.splits.size, callSite, false)
      })
      assertDagSchedulerEmpty(dagScheduler)
    } finally {
      dagScheduler.stop()
      sc.stop()
      // pause to let dagScheduler stop (separate thread)
      Thread.sleep(10)
    }
  }
}
