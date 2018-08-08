(defproject euro-edn-conference-program "0.1.0-SNAPSHOT"
  :description "A simple program to generate the static data file  (in EDN format) used by the euro-online.org conference program app"
  :url "https://www.euro-online.org/conf/"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.layerware/hugsql "0.4.9"]
                 [mysql/mysql-connector-java "8.0.12"]]
  :main ^:skip-aot euro-edn-conference-program.core
  :source-paths ["src" "env"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
