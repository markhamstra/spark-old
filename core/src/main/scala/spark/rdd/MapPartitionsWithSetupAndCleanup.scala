package spark.rdd

import spark.{TaskContext, Partition, RDD}
import spark.RDD.PartitionMapper

/**
 *
 */

class MapPartitionsWithSetupAndCleanup[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    m: PartitionMapper[T,U],
    preservesPartitioning: Boolean
) extends RDD[U](prev){

  override def getPartitions = firstParent[T].partitions

  override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def compute(split: Partition, context: TaskContext) = {
    context.addOnCompleteCallback(m.cleanup _)
    m.setup(split.index)
    firstParent[T].iterator(split, context).map(m.map _)
  }

}
