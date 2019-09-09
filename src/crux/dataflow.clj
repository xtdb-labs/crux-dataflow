(ns crux.dataflow
  (:require
   [clojure.spec.alpha :as s]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clj-3df.core :as df]
   [manifold.stream]
   [crux.api :as api]
   [crux.codec :as c]
   [crux.io :as cio])
  (:import java.io.Closeable
           java.io.File
           java.nio.ByteBuffer))

;; TODO: This breaks range queries for longs.
(extend-protocol c/IdToBuffer
  Long
  (id->buffer [^Long this to]
    (c/id-function
     to (.array (doto (ByteBuffer/allocate Long/BYTES)
                  (.putLong this))))))

(defn- validate-schema!
  [crux schema tx-ops]
  (doseq [[tx-op {:keys [crux.db/id] :as doc}] tx-ops]
    (case tx-op
      :crux.tx/put (do (assert (instance? Long id))
                       (assert (map? doc))
                       (doseq [[k v] (dissoc doc :crux.db/id)
                               :let [{:keys [db/valueType input_semantics]} (get schema k)]]
                         (assert (contains? schema k))
                         (case input_semantics
                           "CardinalityMany"
                           (case valueType
                             (:Eid :Number) (do (assert (coll? v))
                                                (doseq [i v]
                                                  (assert (number? i))))
                             :String (do (assert (coll? v))
                                         (doseq [i v]
                                           (assert (string? i)))))

                           "CardinalityOne"
                           (case valueType
                             (:Eid :Number) (assert (number? v))
                             :String (assert (string? v)))))))))

(defn- index-to-3df
  [conn db crux {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id]}]
  (let [crux-db (api/db crux)]
    (with-open [snapshot (api/new-snapshot crux-db)]
      (let [new-transaction
            (reduce
             (fn [acc [op-key doc-or-id]]
               (case op-key
                 :crux.tx/put (let [new-doc doc-or-id
                                    _ (log/debug "NEW-DOC:" new-doc)
                                    eid (:crux.db/id new-doc)
                                    old-doc (some->> (api/history-descending crux-db snapshot (:crux.db/id new-doc))
                                                     ;; NOTE: This comment seems like a potential bug?
                                                     ;; history-descending inconsistently includes the current document
                                                     ;; sometimes (on first transaction attleast
                                                     (filter
                                                      (fn [entry] (not= (:crux.tx/tx-id entry) tx-id)))
                                                     first :crux.db/doc)]
                                (into
                                 acc
                                 (apply
                                  concat
                                  (for [k (set (concat
                                                (keys new-doc)
                                                (keys old-doc)))
                                        :when (not= k :crux.db/id)]
                                    (let [old-val (get old-doc k)
                                          new-val (get new-doc k)
                                          old-set (when old-val (if (coll? old-val) (set old-val) #{old-val}))
                                          new-set (when new-val (if (coll? new-val) (set new-val) #{new-val}))]
                                      (log/debug "KEY:" k old-set new-set)
                                      (concat
                                       (for [new new-set
                                             :when new
                                             :when (or (nil? old-set) (not (old-set new)))]
                                         [:db/add eid k new])
                                       (for [old old-set
                                             :when old
                                             :when (or (nil? new-set) (not (new-set old)))]
                                         [:db/retract eid k old])))))))))
             []
             tx-ops)]
        (log/debug "3DF Tx:" new-transaction)
        @(df/exec! conn (df/transact db new-transaction))))))

(defrecord CruxDataflowTxListener [conn db crux ^Thread worker-thread ^Process server-process]
  Closeable
  (close [_]
    (manifold.stream/close! (:ws conn))
    (doto worker-thread
      (.interrupt)
      (.join))
    (when server-process
      (doto server-process
        (.destroy)
        (.waitFor)))))

(s/def :crux.dataflow/url string?)
(s/def :crux.dataflow/schema map?)
(s/def :crux.dataflow/poll-interval nat-int?)
(s/def :crux.dataflow/debug-connection? boolean?)
(s/def :crux.dataflow/embed-server? boolean?)

(s/def :crux.dataflow/tx-listener-options (s/keys :req [:crux.dataflow/url
                                                        :crux.dataflow/schema]
                                                  :opt [:crux.dataflow/poll-interval
                                                        :crux.dataflow/debug-connection?
                                                        :crux.dataflow/embed-server?]))

(def ^:const declerative-server-resource "declarative-server-v0.2.0-x86_64-unknown-linux-gnu")

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
    (with-open [in (io/input-stream (io/resource declerative-server-resource))
                out (io/output-stream server-file)]
      (io/copy in out))
    (.setExecutable server-file true)
    (->> (ProcessBuilder. ^"[Ljava.lang.String;" (into-array [(.getAbsolutePath server-file)]))
         (.inheritIO)
         (.start))))

(defn start-dataflow-tx-listener ^java.io.Closeable [crux-node
                                                     {:keys [crux.dataflow/url
                                                             crux.dataflow/schema
                                                             crux.dataflow/poll-interval
                                                             crux.dataflow/debug-connection?
                                                             crux.dataflow/embed-server?]
                                                      :or {poll-interval 100
                                                           debug-connection? false}
                                                      :as options}]
  (s/assert :crux.dataflow/tx-listener-options options)

  (let [server-process (when embed-server?
                         (start-dataflow-server))
        conn ((if debug-connection?
                df/create-debug-conn!
                df/create-conn!) url)
        db (df/create-db schema)]
    (df/exec! conn (df/create-db-inputs db))
    (let [worker-thread (doto (Thread.
                               ^Runnable (fn []
                                           (try
                                             (loop [tx-id -1]
                                               (let [tx-id (with-open [tx-log-context (api/new-tx-log-context crux-node)]
                                                             (reduce
                                                              (fn [_ {:keys [crux.tx/tx-id] :as tx-log-entry}]
                                                                (index-to-3df conn db crux-node tx-log-entry)
                                                                tx-id)
                                                              tx-id
                                                              (api/tx-log crux-node tx-log-context (inc tx-id) true)))]
                                                 (Thread/sleep poll-interval)
                                                 (recur (long tx-id))))
                                             (catch InterruptedException ignore))))
                          (.setName "crux.dataflow.worker-thread")
                          (.start))]
      (->CruxDataflowTxListener conn db crux-node worker-thread server-process))))
