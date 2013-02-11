package spark.api.java;

import scala.Tuple2;

import java.io.Serializable;

public abstract class JavaPairPartitionMapper<T, K, V> implements Serializable {

    public abstract void setup(int partition);

    public abstract Tuple2<K,V> map(T t) throws Exception;

    public abstract void cleanup();
}
