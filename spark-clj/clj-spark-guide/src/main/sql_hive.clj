(ns main.sql-hive
  (:require [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [sparkling.sql :as sql]
            [sparkling.sql.types :as types]);
  (:import [org.apache.spark.sql SparkSession Encoders]
           [org.apache.spark.api.java.function MapFunction];
           [org.apache.spark.api.java JavaSparkContext]))

(let [sb (doto (SparkSession/builder)
           (.master "local[*]")
           (.appName "Sql Hive")
           .enableHiveSupport)
      ss   (.getOrCreate sb)
      sql-c	(sql/sql-context ss)
      jsc (new JavaSparkContext (.sparkContext ss))
      udf-map (reify MapFunction
                (call [_ row]
                  (let [key (.getInt row 0)
                        value (.getString row 1)]
                    (str "key: " key ", value: " value))))
      _ (sql/sql "CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive" sql-c)
      _ (sql/sql "load data local inpath 'src/main/resources/kv1.txt' OVERWRITE into table src" sql-c)
      sql-ds (sql/sql "select key, value from src where key < 10 order by key" sql-c)

      rc-schema
      [{:name      "key"
        :type      (types/long-type)
        :nullable? false}
       {:name      "value"
        :type      (types/string-type)
        :nullable? false}]
      records-df (->> (range 1 100)
                      (map #(vector % (str "rc_val_" %)))
                      (spark/parallelize jsc)
                      (spark/map types/create-row)
                      (sql/rdd->data-frame sql-c (types/struct-type rc-schema)))
      _ (.createOrReplaceTempView records-df
                                  "records")]
  (sql/show (sql/sql "SELECT * FROM src ORDER by key DESC" sql-c))
  (sql/show (sql/sql "SELECT COUNT(*) FROM src" sql-c))
  (sql/show (.map sql-ds udf-map (Encoders/STRING)))
  (sql/show (sql/sql "SELECT * FROM records r JOIN src s ON r.key = s.key" sql-c)));
