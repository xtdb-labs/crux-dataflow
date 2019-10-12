(defproject juxt/crux-3df "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [juxt/crux-core "19.09-1.4.0-alpha"]
                 [juxt/crux-rocksdb "19.09-1.4.0-alpha"]
                 [com.sixthnormal/clj-3df "0.1.0-alpha"]
                 ;[juxt/crux-dataflow "19.09-1.4.1-alpha-SNAPSHOT"]
                 [ch.qos.logback/logback-classic "1.2.3"]]
  :global-vars {*warn-on-reflection* true}
  :main crux-3df.core
  :source-paths
  ["src" "../src"]
  :profiles
  {:dev {:repl-options {:port 55987}}})
