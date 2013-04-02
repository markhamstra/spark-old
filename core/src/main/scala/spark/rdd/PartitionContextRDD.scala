package spark.rdd

import spark.{RDD, Partition, TaskContext}
import spark.PartitionContext

abstract class PartitionContextRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T],
  partitionContext: PartitionContext[T, U],
  preservesPartitioning: Boolean
  ) extends RDD[U](prev) {

  override def getPartitions = firstParent[T].partitions

  override val partitioner = if (preservesPartitioning) prev.partitioner else None
}
