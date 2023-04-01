(defproject clj-spark-guide "2.0.0-SNAPSHOT"
  :description "A Sample project demonstrating the use of Sparkling (https://gorillalabs.github.io/sparkling/), as shown in tutorial https://gorillalabs.github.io/sparkling/articles/tfidf_guide.html"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [gorillalabs/sparkling "3.2.1"]]

  :aot [#".*" sparkling.serialization sparkling.destructuring]
  :main main.app
  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.12 "3.3.2"]
                                       [org.apache.spark/spark-sql_2.12 "3.3.2"]]}
             :dev {:plugins [[lein-dotenv "RELEASE"]]
                   :resource-paths ["src/main/resources/"]}})

