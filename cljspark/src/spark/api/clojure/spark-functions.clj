(in-ns 'spark.api.clojure.core)

(let [factory (spark.api.clojure.FunctionFactory.)]
  (defn spark-function
    "Create a spark.api.java.function.Function from a clojure function"
    [f]
    (.sparkFunction factory f))
  (defn spark-function2
    "Create a spark.api.java.function.Function2 from a clojure function"
    [f]
    (.sparkFunction2 factory f))
  (defn spark-flat-map-function
    "Create a spark.api.java.function.FlatMapFunction from a clojure function"
    [f]
    (.sparkFlatMapFunction factory f))
  (defn spark-pair-function
    "Create a spark.api.java.function.PairFunction from a clojure function whose result is a collection."
    [f]
    (.sparkPairFunction factory f))
  (defn spark-pair-flat-map-function
    "Create a spark.api.java.function.PairFlatMapFunction from a clojure function whose result is a collection of collections."
    [f]
    (.sparkPairFlatMapFunction factory f))
  )

(defmulti spark-function-multi :kv)

(defmethod spark-function-multi nil [f] (spark-function f))

(defmethod spark-function-multi :default [f] (spark-pair-function (:kv f)))

(defmulti spark-flat-map-function-multi :kv)

(defmethod spark-flat-map-function-multi nil [f] (spark-flat-map-function f))

(defmethod spark-flat-map-function-multi :default [f] (spark-pair-flat-map-function (:kv f)))
