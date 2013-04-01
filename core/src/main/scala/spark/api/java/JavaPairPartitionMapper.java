package spark.api.java;

import scala.Tuple2;

public abstract class JavaPairPartitionMapper<T, K, V> implements spark.RDD.PartitionMapper<T, Tuple2<K,V>> {
}
