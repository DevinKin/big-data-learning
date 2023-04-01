(ns main.app
  (:gen-class)
  (:require [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [clojure.string :as str]))

(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "Simple Application")))

(defn -main []
  (spark/with-context sc c
    (let [log-file "/opt/bitnami/spark/README.md"
          log-data (->> log-file
                        (spark/text-file sc)
                        (spark/cache))]

      (prn (str "Lines with a: " (->> log-data
                                      (spark/filter #(str/includes? % "a"))
                                      (spark/count))
                ", lines with b: " (->> log-data
                                        (spark/filter #(str/includes? % "a"))
                                        (spark/count)))))))