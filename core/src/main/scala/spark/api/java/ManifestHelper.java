package spark.api.java;

import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;

class ManifestHelper {

    public static <R> ClassManifest<R> fakeManifest() {
        return (ClassManifest<R>) ClassManifest$.MODULE$.fromClass(Object.class);
    }
}
