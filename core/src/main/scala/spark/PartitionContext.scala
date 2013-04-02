package spark

/**
 * Defines a map function over elements of an RDD in a context with extra setup and cleanup per partition
 */
trait PartitionContext[T, U] extends Serializable {
  /**
   * called at the start of processing of each partition
   */
  def setup(partiton:Int)

  /**
   * transform one element of the partition
   */
  @throws(classOf[Exception]) //for the java api
  def func(t: T) : U

  /**
   * called at the end of each partition.  This will get called even if the map failed (eg., an exception was thrown)
   */
  def cleanup
}

abstract class MapContext[T, U] extends PartitionContext[T, U]
abstract class FlatMapContext[T, U] extends PartitionContext[T, U] {
  override def func(t: T): Seq[U]
}
abstract class FilterContext[T] extends PartitionContext[T, T] {
  override def func(t: T): Boolean
}

object FilterContext {
  implicit def filterContext2PartitionContext[T](filterContext: FilterContext[T]): PartitionContext[T, T] = new PartitionContext[T, T] {
    override def setup(partition: Int) = filterContext.setup(partition)
    override def func(t: T): Boolean = filterContext.func(t)
    override def cleanup = filterContext.cleanup
  }
}
