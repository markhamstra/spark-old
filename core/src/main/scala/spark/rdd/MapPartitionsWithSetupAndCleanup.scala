package spark.rdd

import spark.{TaskContext, Split, RDD}
import spark.RDD.PartitionMapper

/**
 *
 */

class MapPartitionsWithSetupAndCleanup[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    m: PartitionMapper[T,U],
    preservesPartitioning: Boolean
) extends RDD[U](prev){

  override def getSplits = firstParent[T].splits

  override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def compute(split: Split, context: TaskContext) = {
    context.addOnCompleteCallback(m.cleanup _)
    m.setup(split.index)
    firstParent[T].iterator(split, context).map(m.map _)
  }

}
