package spark.rdd

import spark.{RDD, Partition, TaskContext}
import scala.reflect.ClassTag


private[spark]
class FlatMappedRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: T => TraversableOnce[U])
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).flatMap(f)
}
