(ns spark.api.clojure.core
  (:import (java.util ArrayList))
  (:import (spark.api.java JavaSparkContext))
  (:import (spark AccumulatorParam))
  (:use (spark.api.clojure FunctionFactory)))

(defn- spark-context
  "Create a new Spark context."
  ([sc] ;; Scala SparkContext
     (JavaSparkContext. sc))
  ([^String master ^String framework-name]
     (JavaSparkContext. master framework-name))
  ;; ([^String master ^String framework-name ^String spark-home ^String jar-filename]
  ;;    (JavaSparkContext. master framework-name spark-home jar-filename))
  ([^String master ^String framework-name ^String spark-home & jars]
     (if (nil? jars)
       (JavaSparkContext. master framework-name spark-home "")
       (if (= 1 (count jars))
         (if (coll? (first jars))
           (JavaSparkContext. master framework-name spark-home (into-array (first jars)))
           (JavaSparkContext. master framework-name spark-home jars))
         (JavaSparkContext. master framework-name spark-home (into-array jars))))))
  ;; ([^String master ^String framework-name ^String spark-home & jars]
  ;;    (if (nil? jars)
  ;;      (JavaSparkContext. master framework-name spark-home "")
  ;;      (if (and (= 1 (count jars)) (coll? (first jars)))
  ;;        (JavaSparkContext. master framework-name spark-home (into-array (first jars))))))

(defn wrappers!
  "Initialize wrapper functions so that 'map', 'reduce', 'filter', etc. can be used with RDDs in addition to Clojure collections."
  [] (use '(spark.api.clojure wrappers)))


(let [jsc (ref nil)]

  (defn set-spark-context!
  "Set the Spark context."
  ([sc] ;; Scala SparkContext
     (dosync
      (ref-set jsc (spark-context sc))))
  ([^String master ^String framework-name]
     (dosync
      (ref-set jsc (spark-context master framework-name))))
  ([^String master ^String framework-name ^String spark-home ^String jar-filename]
     (dosync
      (ref-set jsc (spark-context master framework-name spark-home jar-filename)))))
  ;; ([^String master ^String framework-name ^String spark-home & jars]
  ;;    (dosync
  ;;     (ref-set jsc (spark-context master framework-name spark-home jars)))))

  ;;   val env = sc.env
  (defn env []
    (.env (.sc @jsc)))

  (defn parallelize
    "Parallelize a collection into a JavaRDD"
    ([coll ^Integer num-slices]
       (.parallelize @jsc (ArrayList. coll) num-slices))
    ([coll]
       (.parallelize @jsc (ArrayList. coll))))

  (defn parallelize-pairs
  "Parallelize a collection of scala.Tuple2 into a JavaPairRDD"
  ([jsc pairs num-slices]
     (.parallelizePairs @jsc (java.util.ArrayList pairs) num-slices))
  ([jsc pairs]
     (.parallelizePairs @jsc (ArrayList. pairs))))

  (defn parallelize-doubles
    "Parallelize a collection of Doubles into a JavaDoubleRDD"
    ([^doubles dbls ^Integer num-slices]
       (.parallelizeDoubles @jsc (ArrayList. dbls) num-slices))
    ([^doubles dbls]
       (.parallelizeDoubles @jsc (ArrayList. dbls))))

  (defn text-file
    "Create an RDD from a text file."
    ([^String path min-splits]
       (.textFile @jsc path (int min-splits)))
    ([^String path]
       (.textFile @jsc path)))

  (defn sequence-file
    "Create a JavaPairRDD from a Hadoop SequenceFile with the given key and value types."
    ([path key-class value-class min-splits]
       (.sequenceFile @jsc path key-class value-class (int min-splits)))
    ([path key-class value-class]
       (.sequenceFile @jsc path key-class value-class)))

  (defn object-file
    "Load an RDD saved as a SequenceFile containing serialized objects with NullWritable keys and
BytesWritable values that contain a serialized partition.  This is still an experimental storage
format and may not be supported exactly as is in future Spark releases.  It will also be pretty
slow if you use the default serializer (Java serialization), though the nice thing about it is
that there's very littl effort required to save arbitrary objects."
    ([path min-splits]
       (.objectFile @jsc path (int min-splits)))
    ([path]
       (.objectFile @jsc path)))

  (defn hadoop-RDD
    "Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf giving its InputFormat and any
other necessary info (e.g. file name for filesystem-based dataset, table name for HyperTable,
etc.)"
    ([conf input-format-class key-class value-class min-splits]
       (.hadoopRDD @jsc conf input-format-class key-class value-class (int min-splits)))
    ([conf input-format-class key-class value-class]
       (.hadoopRDD @jsc conf input-format-class key-class value-class)))

  (defn hadoop-file
    "Create a JavaPairRDD for a Hadoop-readable dataset from a Hadoop JobConf, giving its InputFormat
and any other necessary info (e.g. file name for a filesystem-based dataset, table name for
HyperTable, etc.)"
    ([path input-format-class key-class value-class min-splits]
       (.hadoopFile @jsc path input-format-class key-class value-class (int min-splits)))
    ([path input-format-class key-class value-class]
       (.hadoopFile @jsc path input-format-class key-class value-class)))

  (defn new-api-hadoop-file
    "Create a JavaPairRDD from a given Hadoop file with an arbitrary new API
InputFormat and extra configuration options to pass to the input format."
    [path f-class k-class v-class conf]
    (.newAPIHadoopFile @jsc path f-class k-class v-class conf))

  (defn new-api-hadoop-RDD
    "Create a JavaPairRDD from a given Hadoop file with an arbitrary new API
InputFormat and extra configuration options to pass to the input format."
    [conf f-class k-class v-class]
    (.newAPIHadoopRDD @jsc conf f-class k-class v-class))

  (defn int-accumulator [initial-value]
    (.intAccumulator @jsc initial-value))

  (defn double-accumulator [initial-value]
    (.doubleAccumulator @jsc initial-value))

  (defn accumulator [initial-value accumulator-param]
    (.accumulator @jsc initial-value accumulator-param))

  (defn broadcast [value]
    (.broadcast (.sc @jsc) value))

  (defn stop-spark-context! []
    (dosync
     (.stop @jsc)
     (ref-set jsc nil)))

  (defn get-spark-home []
    (let [opt (.getSparkHome @jsc)]
      (if (.isEmpty opt)
        nil
        (.get opt))))

  (defn get-spark-context
    "Get the SparkContext from the current JavaSparkContext"
    []
    (.sc @jsc)))

(defn union
  "Union of RDDs"
  [rdd & rdds]
  (if (nil? rdds)
    (.union (JavaSparkContext/fromSparkContext (.context (first rdd))) (first rdd) (ArrayList. (rest rdd)))
    (.union (JavaSparkContext/fromSparkContext (.context rdd)) rdd (ArrayList. (flatten rdds)))))

(defn from-spark-context
  "Create JavaSparkContext from a SparkContext"
  [sc]
  (JavaSparkContext. sc))

(defn- arg-count [f]
  (let [m (first (.getDeclaredMethods (class f)))
        p (.getParameterTypes m)]
    (alength p)))

;; end of SparkContext stuff


(load "spark-functions")

(defn spark-map [f rdd]
  (.map rdd (spark-function-multi f)))

(defn flat-map [f rdd]
  (.flatMap rdd (spark-flat-map-function-multi f)))

(defn spark-reduce [f rdd]
  (.reduce rdd (spark-function2 f)))

(defn reduce-by-key [f rdd]
  (.reduceByKey rdd (spark-function2 f)))

(defn spark-filter [f rdd]
  (.filter rdd (spark-function f)))

(defn collect [rdd]
  (.collect rdd))

(defn spark-take [n rdd]
  (.take rdd n))

(defn cache [rdd]
  (.cache rdd))

(defn persist [new-level rdd]
  (.persist rdd new-level))

(defn spark-distinct [rdd]
  (.distinct rdd))

(defn sample [with-replacement fraction seed rdd]
  (.sample rdd (boolean with-replacement) (double fraction) (int seed)))

(defn spark-count [rdd]
  (.count rdd))

(defn count-approx
  ([timeout rdd]
     (.countApprox rdd timeout))
  ([timeout confidence rdd]
     (.countApprox rdd timeout (double confidence))))

(defn count-by-value [rdd]
  (.countByValue rdd))

(defn spark-first [rdd]
  (.first rdd))

(defn get-rdd [rdd]
  (.rdd rdd))

(defn context [rdd]
  (.context rdd))

(defn id [rdd]
  (.id rdd))

(defn get-storage-level [rdd]
  (.getStorageLevel rdd))

(defn splits [rdd]
  (.splits rdd))

(load "conversions")