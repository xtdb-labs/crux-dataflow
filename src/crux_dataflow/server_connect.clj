(ns crux-dataflow.server-connect
  "Manage dataflow server connection or start one from a bundled resource"
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [crux.io :as cio]))


(def ^:const default-dataflow-server-url "ws://127.0.0.1:6262")

(def ^:const declarative-server-resource "declarative-server-v0.2.0-x86_64-unknown-linux-gnu")
; todo allow to manage external server processes

(defn- linux? []
  (str/includes? (str/lower-case (System/getProperty "os.name")) "linux"))

(defn- x86_64? []
  (let [arch (System/getProperty "os.arch")]
    (or (= "x86_64" arch) (= "amd64" arch))))

(defn start-dataflow-server []
  (assert (and (linux?) (x86_64?))
          "Only x86_64-unknown-linux-gnu supported")
  (let [server-file (io/file (cio/create-tmpdir "declarative-server")
                             "declarative-server")]
    (with-open [in (io/input-stream (io/resource declarative-server-resource))
                out (io/output-stream server-file)]
      (io/copy in out))
    (.setExecutable server-file true)
    (->> (ProcessBuilder. ^"[Ljava.lang.String;" (into-array [(.getAbsolutePath server-file)]))
         (.inheritIO)
         (.start))))
