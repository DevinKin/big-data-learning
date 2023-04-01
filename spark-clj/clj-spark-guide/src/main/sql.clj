(ns main.sql
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.sql :as sql]
            [clojure.string :as str]
            [sparkling.sql.types :as types]))


(def c (-> (conf/spark-conf) ; create a new config
           (conf/master "local[*]") ; set the master
           (conf/app-name "SQL")))

(spark/with-context sc c
  (let [sql-c (sql/sql-context sc)
        pdf (sql/read-json sql-c "src/main/resources/people.json")
        ptdf (spark/text-file sc "src/main/resources/people.txt" 1)

        schema
        [{:name      "name"
          :type      (types/string-type)
          :nullable? false}
         {:name      "age"
          :type      (types/string-type)
          :nullable? false}]]
    (->> pdf
         (sql/select-expr ["name" "age + 1 as age"])
         (sql/show))

    (sql/print-schema pdf)

    (sql/show (sql/select (sql/cols ["name", "age"] pdf) pdf))

    (->> pdf
         (sql/select-expr ["name" "age + 1 as age"])
         (sql/show))


    (->> pdf
         (sql/selects ["age"])
         (sql/where "age > 21")
         (sql/show))

    (->> pdf
         (sql/group-by-cols ["age"])
         .count
         (sql/show))

    (.createOrReplaceTempView (->> ptdf
                                   (spark/map (fn [line]
                                                (let [parts (str/split line #", ")]
                                                  [(-> parts
                                                       (get 0))
                                                   (-> parts
                                                       (get 1)
                                                       (str/trim))])))
                                   (spark/map types/create-row)
                                   (sql/rdd->data-frame sql-c (types/struct-type schema)))
                              "people")


    (sql/show (sql/sql "SELECT name FROM people WHERE age BETWEEN 13 AND 19" sql-c))))





