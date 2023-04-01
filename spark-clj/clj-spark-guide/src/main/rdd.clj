(ns main.rdd
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [clojure.string :as str]
            [sparkling.accumulator :as sacc]))

(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "RDD")))

(spark/with-context sc c
  (let [tf (spark/text-file sc "src/main/resources/README.md")
        accum (sacc/long-accumulator sc 0 "my-accumulator")]

    (->> tf
         (spark/map-to-pair (fn [w] (spark/tuple w 1)))
         (spark/reduce-by-key +))


    (->> [1 2 3 4]
         (spark/parallelize sc)
         (spark/foreach (fn [x] (.add accum x))))

    (.value accum)))
