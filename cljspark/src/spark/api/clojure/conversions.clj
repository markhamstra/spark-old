(in-ns 'spark.api.clojure.core)

(defn- scala-tuple2-to-map
  "Convert a collection of Scala Tuple2 into a map where tuple2._1 is the key associated with value tuple2._2"
  [tuple2s map-constructor]
  (apply map-constructor (flatten (for [tuple2 tuple2s] (list (._1 tuple2) (._2 tuple2))))))

(defn scala-tuple2-to-hash-map
  "Convert a collection of Scala Tuple2 into a hash-map where tuple2._1 is the key associated with value tuple2._2"
  [& tuple2s]
  (scala-tuple2-to-map tuple2s hash-map))

(defn scala-tuple2-to-array-map
  "Convert a collection of Scala Tuple2 into an array-map where tuple2._1 is the key associated with value tuple2._2"
  [& tuple2s]
  (scala-tuple2-to-map tuple2s array-map))

(defn map-entry-to-scala-tuple2
  "Create a Scala Tuple2 from a map with a single map entry"
  [kv]
  (scala.Tuple2. (key kv) (val kv)))

(defn scala-tuple2
  "Create a Scala Tuple2 from a single entry map"
  [m]
  (apply map-entry-to-scala-tuple2 m))

(defn- map-to-scala-tuple2
  "Convert a map into a collection of Scala Tuple2"
  [the-map collection-constructor]
  (apply collection-constructor (for [kv the-map] (map-entry-to-scala-tuple2 kv))))

(defn map-to-list-of-scala-tuple2
  "Convert a map into a list of Scala Tuple2"
  [the-map]
  (map-to-scala-tuple2 the-map list))

(defn map-to-vector-of-scala-tuple2
  "Convert a map into a vector of Scala Tuple2"
  [the-map]
  (map-to-scala-tuple2 the-map vector))

(defn map-to-array-of-scala-tuple2
  "Convert a map into a Java array of Scala Tuple2"
  [the-map]
  (into-array scala.Tuple2 (map-to-vector-of-scala-tuple2 the-map)))