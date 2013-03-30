package spark.deploy.master

private[spark] object WorkerState extends Enumeration {
  val ALIVE, DEAD, DECOMMISSIONED = Value
  type WorkerState = Value
}
