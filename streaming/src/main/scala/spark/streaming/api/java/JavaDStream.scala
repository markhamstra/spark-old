package spark.streaming.api.java

import spark.streaming.{Duration, Time, DStream}
import spark.api.java.function.{Function => JFunction}
import spark.api.java.JavaRDD
import spark.storage.StorageLevel
import spark.RDD
import scala.reflect.ClassTag

/**
 * A Discretized Stream (DStream), the basic abstraction in Spark Streaming, is a continuous
 * sequence of RDDs (of the same type) representing a continuous stream of data (see [[spark.RDD]]
 * for more details on RDDs). DStreams can either be created from live data (such as, data from
 * HDFS, Kafka or Flume) or it can be generated by transformation existing DStreams using operations
 * such as `map`, `window` and `reduceByKeyAndWindow`. While a Spark Streaming program is running, each
 * DStream periodically generates a RDD, either from live data or by transforming the RDD generated
 * by a parent DStream.
 *
 * This class contains the basic operations available on all DStreams, such as `map`, `filter` and
 * `window`. In addition, [[spark.streaming.api.java.JavaPairDStream]] contains operations available
 * only on DStreams of key-value pairs, such as `groupByKeyAndWindow` and `join`.
 *
 * DStreams internally is characterized by a few basic properties:
 *  - A list of other DStreams that the DStream depends on
 *  - A time interval at which the DStream generates an RDD
 *  - A function that is used to generate an RDD after each time interval
 */
class JavaDStream[T](val dstream: DStream[T])(implicit val classManifest: ClassTag[T])
    extends JavaDStreamLike[T, JavaDStream[T], JavaRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  /** Return a new DStream containing only the elements that satisfy a predicate. */
  def filter(f: JFunction[T, java.lang.Boolean]): JavaDStream[T] =
    dstream.filter((x => f(x).booleanValue()))

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaDStream[T] = dstream.cache()

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaDStream[T] = dstream.persist()

  /** Persist the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaDStream[T] = dstream.persist(storageLevel)

  /** Generate an RDD for the given duration */
  def compute(validTime: Time): JavaRDD[T] = {
    dstream.compute(validTime) match {
      case Some(rdd) => new JavaRDD(rdd)
      case None => null
    }
  }

  /**
   * Return a new DStream in which each RDD contains all the elements in seen in a
   * sliding window of time over this DStream. The new DStream generates RDDs with
   * the same interval as this DStream.
   * @param windowDuration width of the window; must be a multiple of this DStream's interval.
   */
  def window(windowDuration: Duration): JavaDStream[T] =
    dstream.window(windowDuration)

  /**
   * Return a new DStream in which each RDD contains all the elements in seen in a
   * sliding window of time over this DStream.
   * @param windowDuration width of the window; must be a multiple of this DStream's
   *                       batching interval
   * @param slideDuration  sliding interval of the window (i.e., the interval after which
   *                       the new DStream will generate RDDs); must be a multiple of this
   *                       DStream's batching interval
   */
  def window(windowDuration: Duration, slideDuration: Duration): JavaDStream[T] =
    dstream.window(windowDuration, slideDuration)

  /**
   * Return a new DStream by unifying data of another DStream with this DStream.
   * @param that Another DStream having the same interval (i.e., slideDuration) as this DStream.
   */
  def union(that: JavaDStream[T]): JavaDStream[T] =
    dstream.union(that.dstream)
}

object JavaDStream {
  implicit def fromDStream[T: ClassTag](dstream: DStream[T]): JavaDStream[T] =
    new JavaDStream[T](dstream)
}
