package spark.api.java;

import java.io.Serializable;

public abstract class JavaDoublePartitionMapper<T> implements Serializable {

    public abstract void setup(int partition);

    public abstract Double map(T t) throws Exception;

    public abstract void cleanup();
}
