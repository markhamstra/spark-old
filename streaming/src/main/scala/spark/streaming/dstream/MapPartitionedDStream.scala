package spark.streaming.dstream

import spark.streaming.{Duration, DStream, Time}
import spark.RDD
import scala.reflect.ClassTag

private[streaming]
class MapPartitionedDStream[T: ClassTag, U: ClassTag](
    parent: DStream[T],
    mapPartFunc: Iterator[T] => Iterator[U],
    preservePartitioning: Boolean
  ) extends DStream[U](parent.ssc) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(_.mapPartitions[U](mapPartFunc, preservePartitioning))
  }
}

