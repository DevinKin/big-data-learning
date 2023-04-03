(defproject clj-spark-guide "2.0.0-SNAPSHOT"
  :description "A Sample project demonstrating the use of Sparkling (https://gorillalabs.github.io/sparkling/), as shown in tutorial https://gorillalabs.github.io/sparkling/articles/tfidf_guide.html"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [gorillalabs/sparkling "3.2.1"]
                 [org.apache.parquet/parquet-hadoop-tests "1.12.0"]
                 [org.codehaus.jackson/jackson-mapper-asl "1.9.13"]
                 [org.postgresql/postgresql "42.5.1"]
                 [com.fasterxml.jackson.core/jackson-core "2.14.2"]]

  :aot [#".*" sparkling.serialization sparkling.destructuring]
  :main main.app
  :repositories {"local" ~(str (.toURI (java.io.File. "local_repo")))}
  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.12 "3.3.2"]
                                       [org.apache.spark/spark-sql_2.12 "3.3.2"]
                                       [org.apache.spark/spark-hive_2.12 "3.3.2"]
                                       [org.apache.spark/spark-avro_2.12 "3.3.2"]
                                       [org.apache.spark/spark-sql-kafka-0-10_2.12 "3.3.2"]]}
             :dev {:plugins [[lein-dotenv "RELEASE"]]
                   :resource-paths ["src/main/resources/"]}})

