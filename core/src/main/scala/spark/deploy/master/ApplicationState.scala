package spark.deploy.master

private[spark] object ApplicationState extends Enumeration {
  val WAITING, RUNNING, FINISHED, FAILED = Value
  type ApplicationState = Value
  val MAX_NUM_RETRY = 10
}
