(ns spark.api.clojure.FunctionFactory
  (:import (spark.api.java.function Function FlatMapFunction Function2 PairFunction PairFlatMapFunction)
           (scala Tuple2))
  (:gen-class
   :name spark.api.clojure.FunctionFactory
   :methods [[sparkFunction [clojure.lang.IFn] spark.api.java.function.Function]
             [sparkFlatMapFunction [clojure.lang.IFn] spark.api.java.function.FlatMapFunction]
             [sparkFunction2 [clojure.lang.IFn] spark.api.java.function.Function2]
             [sparkPairFunction [clojure.lang.IFn] spark.api.java.function.PairFunction]
             [sparkPairFlatMapFunction [clojure.lang.IFn] spark.api.java.function.PairFlatMapFunction]]))

(defn -sparkFunction [_ f]
  (proxy [Function] []
    (call [a] (f a))))

(defn -sparkFlatMapFunction [_ f]
  (proxy [FlatMapFunction] []
    (call [a] (f a))))

(defn -sparkFunction2 [_ f]
  (proxy [Function2] []
    (call [a b] (f a b))))

(defn -sparkPairFunction [_ f]
  (proxy [PairFunction] []
    (call [a] (let [pair (f a)]
                (Tuple2. (first pair) (second pair))))))

(defn -sparkPairFlatMapFunction [_ f]
  (proxy [PairFlatMapFunction] []
    (call [a] (let [pairs (f a)]
                (map #(Tuple2. (first %) (second %)) pairs)))))