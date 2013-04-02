package spark.rdd

import spark.{TaskContext, Partition, RDD, MapContext}

class MapContextRDD[U: ClassManifest, T: ClassManifest](
  prev: RDD[T],
  mapContext: MapContext[T,U],
  preservesPartitioning: Boolean) extends PartitionContextRDD[U, T](prev, mapContext, preservesPartitioning){
    override def compute(split: Partition, taskContext: TaskContext) = {
      taskContext.addOnCompleteCallback(mapContext.cleanup _)
      mapContext.setup(split.index)
      firstParent[T].iterator(split, taskContext).map(mapContext.func _)
    }
}
