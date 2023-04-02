(ns main.sql-parquet
  (:require [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [clojure.string :as str]
            [sparkling.sql :as sql]
            [sparkling.sql.types :as types])
  (:import (org.apache.parquet.crypto.keytools.mocks InMemoryKMS)))


(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "Sql Parquet")))
; encrypted parquet files
; mvn deploy:deploy-file -Dfile=local_repo/parquet-hadoop-1.12.0-tests.jar -DartifactId=parquet-hadoop -Dversion=1.12.0 -DgroupId=org.apache.parquet -Dpackaging=jar -Durl=file:local_repo

(spark/with-context sc c
  (let [sql-c (sql/sql-context sc)
        people-df (sql/read-json sql-c "src/main/resources/people.json")
        sq-schema
        [{:name      "value"
          :type      (types/long-type)
          :nullable? false}
         {:name      "square"
          :type      (types/long-type)
          :nullable? false}]

        cube-schema
        [{:name      "value"
          :type      (types/long-type)
          :nullable? false}
         {:name      "cube"
          :type      (types/long-type)
          :nullable? false}]
        squares-df (->> (range 1 5)
                        (map #(vector % (* % %)))
                        (spark/parallelize sc)
                        (spark/map types/create-row)
                        (sql/rdd->data-frame sql-c (types/struct-type sq-schema)))

        cubes-df (->> (range 6 10)
                      (map #(vector % (* % %)))
                      (spark/parallelize sc)
                      (spark/map types/create-row)
                      (sql/rdd->data-frame sql-c (types/struct-type cube-schema)))

        merge-df (-> sql-c
                     .read
                     (.option "mergeSchema" "true")
                     (.parquet "src/main/resources/data/test_table"))

        #_df2 #_(-> sql-c
                    .read
                    (.parquet "src/main/resources/table.parquet.encrypted"))]

    #_(sql/write-parquet "src/main/resources/people.parquet" people-df)

    (-> squares-df
        .write
        (.mode "overwrite")
        (.parquet "src/main/resources/data/test_table/key=1"))

    (-> cubes-df
        .write
        (.mode "overwrite")
        (.parquet "src/main/resources/data/test_table/key=2"))

    (sql/print-schema merge-df)


    (doto (.hadoopConfiguration sc)
      (.set "parquet.encryption.kms.client.class"
            "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")
      (.set "parquet.encryption.key.list"
            "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA==")
      (.set "parquet.crypto.factory.class" ,
            "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"))

    (-> squares-df
        (.write)
        (.mode "overwrite")
        (.option "parquet.encryption.column.keys" , "keyA:square")
        (.option "parquet.encryption.footer.key" , "keyB")
        (.parquet "src/main/resources/table.parquet.encrypted"))


    #_(sql/show df2)))