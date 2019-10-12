(defproject juxt/crux-dataflow "derived-from-git"
  :description "Crux Declarative Differential Dataflow (3DF) Integration"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [com.sixthnormal/clj-3df "0.1.0-alpha"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware]
  :global-vars {*warn-on-reflection* true})
