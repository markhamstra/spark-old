package spark.deploy

private[spark] object ExecutorState extends Enumeration {
  type ExecutorState = Value
  val LAUNCHING, LOADING, RUNNING, KILLED, FAILED, LOST = Value

  def isFinished(state: ExecutorState): Boolean = Seq(KILLED, FAILED, LOST).contains(state)
}
