package spark.api.java;

import scala.Tuple2;
import spark.RDD;

import java.io.Serializable;

public abstract class JavaPairPartitionMapper<T, K, V> implements spark.RDD.PartitionMapper<T, Tuple2<K,V>>, Serializable {

    public abstract void setup(int partition);

    public abstract Tuple2<K,V> map(T t);

    public abstract void cleanup();
}
