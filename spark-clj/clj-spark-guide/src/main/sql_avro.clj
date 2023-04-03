(ns main.sql-avro
  (:require [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [sparkling.sql :as sql]
            [sparkling.sql.types :as types])
  (:import [java.nio.file Files Paths]
           [java.net URI]))

(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "sql-avro")))

(defn- col [cn]
  (org.apache.spark.sql.functions/col cn))

(defn- from_avro [cn schema]
  (org.apache.spark.sql.avro.functions/from_avro (col cn) schema))

(defn- to_avro [cn]
  (org.apache.spark.sql.avro.functions/to_avro (col cn)))

(spark/with-context sc c
  (let [sql-c (sql/sql-context sc)
        users-df (-> sql-c
                     .read
                     (.format "avro")
                     (.load "src/main/resources/users.avro"))
        json-format-schema (str (Files/readAllBytes (Paths/get (URI. "file:///Users/devin/Learning/BigData/spark-clj/clj-spark-guide/src/main/resources/user.avsc"))))

        #_df #_(-> sql-c
                   .readStream
                   (.format "kafka")
                   (.option "kafka.bootstrap.servers" "localhost:9092")
                   (.option "subscribe" "topic1")
                   (.load))
        #_output #_(-> df
                       (.select (into-array [(.as (from_avro "value" json-format-schema) "user")]))
                       (.where "user.favorite_color == \"red\"")
                       (.select (into-array [(.as (to_avro "user.name") "value")])))
        #_query #_(-> output
                      .writeStream
                      (.format "kafka")
                      (.option "kafka.bootstrap.servers" "localhost:9092")
                      (.option "topic" "topic2")
                      (.start))];
    (sql/show users-df)
    (-> (sql/selects ["name" "favorite_color"] users-df)
        .write
        (.format "avro")
        (.mode "overwrite")
        (.save "src/main/resources/namesAndFavColors.avro"))))


