(ns main.datasouce
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.sql :as sql]
            [clojure.string :as str]
            [sparkling.sql.types :as types]))



(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "datasouce")))

(spark/with-context sc c
  (let [sql-c (sql/sql-context sc)
        users-df (sql/read-parquet sql-c "src/main/resources/users.parquet")
        people-df (sql/read-json sql-c "src/main/resources/people.json")
        people-df-csv (-> sql-c
                          .read
                          (.format "csv")
                          (.option "sep" ";")
                          (.option "inferSchema" "true")
                          (.option "header" "true")
                          (.load "src/main/resources/people.csv"))
        sql-df (sql/sql "SELECT * from parquet.`src/main/resources/users.parquet`" sql-c)]
    (-> (sql/selects ["name" "favorite_color"] users-df)
        .write
        (.mode "overwrite")
        (.save "src/main/resources/namesAndFavColors.parquet"))

    (-> (sql/selects ["name" "age"] people-df)
        .write
        (.mode "overwrite")
        (.format "parquet")
        (.save "src/main/resources/namesAndAges.parquet"))

    (-> (sql/selects ["name" "age"] people-df-csv)
        (sql/show))

    (-> users-df
        .write
        (.mode "overwrite")
        (.format "orc")
        (.option "orc.bloom.filter.columns" "favorite_color")
        (.option "orc.dictionary.key.threshold" "1.0")
        (.option "orc.column.encoding.direct" "name")
        (.save "src/main/resources/users_with_options.orc"))

    (-> users-df
        .write
        (.mode "overwrite")
        (.option "parquet.bloom.filter.enabled#favorite_color", true)
        (.option "parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
        (.option "parquet.enable.dictionary", "true")
        (.option "parquet.write-checksum.enabled", "false")
        (.save "src/main/resources/users_with_options.parquet"))


    (sql/show sql-df)

    (-> people-df
        .write
        ;(.bucketBy 42 "name")
        ;(.sortBy "age")
        (.format "parquet")
        (.option "path" "file:/Users/devin/Learning/BigData/Spark/clj-spark-guide/spark-warehouse/people_bucketed")
        (.mode "overwrite")
        (.saveAsTable "people_bucketed"))

    (-> users-df
        .write
        (.format "parquet")
        #_(.partitionBy "favorite_color")
        #_(.bucketBy 42 "name")
        (.option "path" "file:/Users/devin/Learning/BigData/Spark/clj-spark-guide/spark-warehouse/users_partitioned_bucketed")

        (.mode "overwrite")
        (.saveAsTable "users_partitioned_bucketed"))))