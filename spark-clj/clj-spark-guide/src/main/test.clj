(ns main.test
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [clojure.string :as str]))

; Create a Spark context and give it a name
(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "clj-spark-guide"))) ; set the app name

(spark/with-context sc c
  (let [data (spark/parallelize sc ["a" "b" "a" "d" "a"])
        rm-data (spark/text-file sc "src/main/resources/README.md")]
    (spark/first data)
    (spark/count (spark/filter #(= "a" %) data))
    (->> rm-data
         (spark/map count)
         (spark/reduce +))
    (->> rm-data
         (spark/map (fn [line]
                      (count (str/split line #"\s"))))
         (spark/reduce #(if (> %1 %2) %1 %2)))
    (def line-spark
      (->> rm-data
           (spark/filter #(str/includes? % "Spark"))))
    #_(->> rm-data
           (spark/flat-map (fn [line]
                             (count (str/split line #"\s"))))
         ;(spark/group-by-key)
           (spark/count))
    (spark/cache line-spark)
    (spark/count line-spark)))