package spark.scheduler.cluster

import spark.{SparkContext, Utils}

/**
 * A backend interface for cluster scheduling systems that allows plugging in different ones under
 * ClusterScheduler. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
private[spark] trait SchedulerBackend {
  def start(): Unit
  def stop(): Unit
  def killTask(taskId: Long, executorId: String): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  // Memory used by each executor (in megabytes)
  protected val executorMemory: Int = SparkContext.executorMemoryRequested
}
