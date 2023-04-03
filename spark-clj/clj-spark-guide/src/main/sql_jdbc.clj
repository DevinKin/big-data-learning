(ns main.sql-jdbc
  (:require [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [sparkling.sql :as sql];
            [sparkling.sql.types :as types]))

(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "sql-jdbc")))

#_(spark/with-context sc c
    (let [sql-c (sql/sql-context sc)
          jdbc-df (-> sql-c
                      .read
                      (.format "jdbc")
                      (.option "url" "jdbc:postgresql://localhost:5432/postgres")
                      (.option "dbtable" "public.spark_pg_tbl")
                      (.option "user" "postgres")
                      (.option "password" "postgres")
                      .load)]
      (sql/show jdbc-df)

      (-> jdbc-df;
          .write
          (.format "jdbc")
          (.option "url" "jdbc:postgresql://localhost:5432/postgres")
          (.option "dbtable" "public.spark_pg_tbl2")
          (.option "user" "postgres")
          (.option "password" "postgres")
          (.option "createTableColumnTypes", "id int, data VARCHAR(1024)")
          .save)))