(ns main.resources.source-option
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.sql :as sql]
            [clojure.string :as str]
            [sparkling.sql.types :as types]))

(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "source-options")))


(spark/with-context sc c
  (let [sql-c (sql/sql-context sc)
        _   (sql/sql "set spark.sql.files.ignoreCorruptFiles=true" sql-c)
        _ (sql/sql "set spark.sql.files.ignoreMissingFiles=true" sql-c)
        test-corrup-df (sql/read-parquet sql-c "src/main/resources/dir1/*")
        test-glob-filter-df (-> sql-c
                                .read
                                (.format "parquet")
                                (.option "pathGlobFilter" "*.parquet")
                                (.load "src/main/resources/dir1"))

        recursive-loaded-df (-> sql-c
                                .read
                                (.format "parquet")
                                (.option "recursiveFileLookup" "true")
                                (.load "src/main/resources/dir1"))
        before-filter-df (-> sql-c
                             .read
                             (.format "parquet")
                             (.option "modifiedBefore", "2020-07-01T05:30:00")
                             (.option "modifiedAfter", "2020-06-01T05:30:00")
                             (.option "timeZone" "CST")
                             (.load "src/main/resources/dir1"))]
    (sql/show test-corrup-df)
    (sql/show test-glob-filter-df)
    (sql/show recursive-loaded-df)
    (sql/show before-filter-df)))