package spark.api.java;

import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;

import java.io.Serializable;

public abstract class JavaPartitionMapper<T,R> implements Serializable {

    public abstract void setup(int partition);

    public abstract R map(T t) throws Exception;

    public abstract void cleanup();

    public ClassManifest<R> returnType() {
        return (ClassManifest<R>) ClassManifest$.MODULE$.fromClass(Object.class);
    }
}
