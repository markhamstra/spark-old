package spark.rdd

import spark.{TaskContext, Partition, RDD, FlatMapContext}

class FlatMapContextRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T],
  flatMapContext: FlatMapContext[T, U],
  preservesPartitioning: Boolean) extends PartitionContextRDD[U, T](prev, flatMapContext, preservesPartitioning){
    override def compute(split: Partition, taskContext: TaskContext) = {
      taskContext.addOnCompleteCallback(flatMapContext.cleanup _)
      flatMapContext.setup(split.index)
      firstParent[T].iterator(split, taskContext).flatMap(flatMapContext.func _)
    }
}
