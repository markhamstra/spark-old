(ns spark.api.clojure.wrappers
  (:use [spark.api.clojure.core :only (spark-map spark-filter spark-take spark-count spark-first spark-reduce)]))

(defn map
  "Call JavaRDD.map on a JavaRDD, else delegate to clojure.core/map"
  ([f coll]
     (if (coll? coll)
       (clojure.core/map f coll)
       (spark-map f coll)))
  ([f coll & colls]
     (apply clojure.core/map f coll colls)))

(defn filter
  "Call JavaRDD.filter on a JavaRDD, else delegate to clojure.core/filter"
  [pred coll]
  (if (coll? coll)
    (clojure.core/filter pred coll)
    (spark-filter pred coll)))

(defn take
  "Call JavaRDD.take on a JavaRDD, else delegate to clojure.core/filter"
  [n coll]
  (if (coll? coll)
    (clojure.core/take n coll)
    (spark-take n coll)))

(defn count
  "Call JavaRDD.count on a JavaRDD, else delegate to clojure.core/count"
  [coll]
  (if (coll? coll)
    (clojure.core/count coll)
    (spark-count coll)))

(defn first
  "Call JavaRDD.first on a JavaRDD, else delegate to clojure.core/first"
  [coll]
  (if (coll? coll)
    (clojure.core/first coll)
    (spark-first coll)))

(defn reduce
  "Call JavaRDD.reduce on a JavaRDD, else delegate to clojure.core/reduce"
  ([f val coll]
     (clojure.core/reduce f val coll))
  ([f coll]
     (if (coll? coll)
       (clojure.core/reduce f coll)
       (spark-reduce f coll))))