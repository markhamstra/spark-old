package spark.scheduler.cluster

import spark.Utils

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
  protected val executorMemory = {
    // TODO: Might need to add some extra memory for the non-heap parts of the JVM
    Option(System.getProperty("spark.executor.memory"))
      .orElse(Option(System.getenv("SPARK_MEM")))
      .map(Utils.memoryStringToMb)
      .getOrElse(512)
  }

}
