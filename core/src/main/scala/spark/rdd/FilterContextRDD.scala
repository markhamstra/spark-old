package spark.rdd

import spark.{TaskContext, Partition, RDD, FilterContext}

/**
 *
 */

class FilterContextRDD[T: ClassManifest](
  prev: RDD[T],
  filterContext: FilterContext[T]) extends PartitionContextRDD[T, T](prev, filterContext, true){
    override def compute(split: Partition, taskContext: TaskContext) = {
      taskContext.addOnCompleteCallback(filterContext.cleanup _)
      filterContext.setup(split.index)
      firstParent[T].iterator(split, taskContext).filter(filterContext.func _)
    }
}
