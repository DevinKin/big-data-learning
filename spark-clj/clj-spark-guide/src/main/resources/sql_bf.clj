(ns main.resources.sql-bf
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.sql :as sql]
            [clojure.string :as str]
            [sparkling.sql.types :as types]))

(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "SQL-BF")))


(spark/with-context sc c
  (let [sql-c (sql/sql-context sc)]
    (sql/show (-> sql-c
                  .read
                  (.format "binaryFile")
                  (.option "pathGlobFilter" "*.png")
                  (.load "src/main/resources/imgs")))))