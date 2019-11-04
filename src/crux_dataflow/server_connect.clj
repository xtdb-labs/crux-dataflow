(ns crux-dataflow.server-connect
  "Manage dataflow server connection or start one from a bundled resource"
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [crux.io :as cio])
  (:import (java.lang ProcessImpl)))


(def ^:const default-dataflow-server-url "ws://127.0.0.1:6262")

(def ^:const declarative-server-resource-linux "declarative-server-v0.2.0-x86_64-unknown-linux-gnu")
(def ^:const declarative-server-resource-macos "declarative-server-v0.2.0-x86_64-macos")
; todo allow to manage external server processes

(def ^{:private true :const true}
  -os-name (str/lower-case (System/getProperty "os.name")))

(def macos? (str/includes? -os-name  "mac os x"))

(def linux? (str/includes? -os-name "linux"))

(defn- get-server-resource []
  (io/resource
    ({"linux" declarative-server-resource-linux
      "mac os x" declarative-server-resource-macos}
     -os-name)))


(defn- x86_64? []
  (let [arch (System/getProperty "os.arch")]
    (or (= "x86_64" arch) (= "amd64" arch))))

(defn ^ProcessImpl start-dataflow-server
  "Starts Declarative Dataflow server process. Only for Linux and Mac OS."
  []
  (assert (and (or macos? linux?) (x86_64?))
          "Only x86_64 unknown-linux-gnu supported")
  (let [server-file (io/file (cio/create-tmpdir "declarative-server")
                             "declarative-server")]
    (with-open [in (io/input-stream (get-server-resource))
                out (io/output-stream server-file)]
      (io/copy in out))
    (.setExecutable server-file true)
    (->> (ProcessBuilder. ^"[Ljava.lang.String;" (into-array [(.getAbsolutePath server-file)]))
         (.inheritIO)
         (.start))))
