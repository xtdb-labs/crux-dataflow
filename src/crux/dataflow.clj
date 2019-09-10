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

(defn- validate-schema! [schema {:keys [crux.db/id] :as doc}]
  (assert (instance? Long id))
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
                      (assert (string? i))))
        :Bool (do (assert (coll? v))
                  (doseq [i v]
                    (assert (boolean? i)))))

      "CardinalityOne"
      (case valueType
        (:Eid :Number) (assert (number? v))
        :String (assert (string? v))
        :Bool (assert (boolean? v))))))

(defn- matches-schema? [schema doc]
  (try
    (validate-schema! schema doc)
    true
    (catch AssertionError e
      (log/debug e "Does not match schema:")
      false)))

(defn- index-to-3df
  [crux-node conn db schema {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id]}]
  (let [crux-db (api/db crux-node tx-time tx-time)]
    (with-open [snapshot (api/new-snapshot crux-db)]
      (let [new-transaction
            (reduce
             (fn [acc [op-key doc-or-id]]
               (case op-key
                 :crux.tx/put (if-not (matches-schema? schema doc-or-id)
                                (do (log/debug "SKIPPING-DOC:" doc-or-id)
                                    acc)
                                (let [new-doc doc-or-id
                                      _ (log/debug "NEW-DOC:" new-doc)
                                      eid (:crux.db/id new-doc)
                                      old-doc (some->> (api/history-descending crux-db snapshot eid)
                                                       ;; NOTE: This comment seems like a potential bug?
                                                       ;; history-descending inconsistently includes the current document
                                                       ;; sometimes (on first transaction attleast
                                                       (filter
                                                        (fn [entry] (not= (:crux.tx/tx-id entry) tx-id)))
                                                       first :crux.db/doc)
                                      _ (log/debug "OLD-DOC:" old-doc)]
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
                                            old-set (when (not (nil? old-val)) (if (coll? old-val) (set old-val) #{old-val}))
                                            new-set (when (not (nil? new-val)) (if (coll? new-val) (set new-val) #{new-val}))]
                                        (log/debug "KEY:" k old-set new-set)
                                        (concat
                                         (for [new new-set
                                               :when (not (nil? new))
                                               :when (not (contains? old-set new))]
                                           [:db/add eid k new])
                                         (for [old old-set
                                               :when (not (nil? old))
                                               :when (not (contains? new-set old))]
                                           [:db/retract eid k old]))))))))))
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
(s/def :crux.dataflow/batch-size nat-int?)
(s/def :crux.dataflow/debug-connection? boolean?)
(s/def :crux.dataflow/embed-server? boolean?)

(s/def :crux.dataflow/tx-listener-options (s/keys :req [:crux.dataflow/schema]
                                                  :opt [:crux.dataflow/url
                                                        :crux.dataflow/poll-interval
                                                        :crux.dataflow/batch-size
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

(def ^:const default-dataflow-server-url "ws://127.0.0.1:6262")

(defn- dataflow-consumer [crux-node conn db from-tx-id {:crux.dataflow/keys [schema
                                                                             poll-interval
                                                                             batch-size]
                                                        :or {poll-interval 100
                                                             batch-size 1000}
                                                        :as options}]
  ;; TODO: From where does this store and get its start position?
  ;; Assume this is related to the state of the running
  ;; declarative-dataflow instance itself?
  (loop [tx-id from-tx-id]
    (let [last-tx-id (with-open [tx-log-context (api/new-tx-log-context crux-node)]
                       (->> (api/tx-log crux-node tx-log-context (inc tx-id) true)
                            (take batch-size)
                            (reduce
                             (fn [_ {:keys [crux.tx/tx-id] :as tx-log-entry}]
                               (index-to-3df crux-node conn db schema tx-log-entry)
                               tx-id)
                             tx-id)))]
      (when (= last-tx-id tx-id)
        (Thread/sleep poll-interval))
      (recur (long last-tx-id)))))

(defn start-dataflow-tx-listener ^java.io.Closeable [crux-node
                                                     {:crux.dataflow/keys [schema
                                                                           url
                                                                           debug-connection?
                                                                           embed-server?]
                                                      :or {url default-dataflow-server-url
                                                           debug-connection? false
                                                           embed-server? false}
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
                               #(try
                                  (dataflow-consumer crux-node conn db -1 options)
                                  (catch InterruptedException ignore)
                                  (catch Throwable t
                                    (log/fatal t "Polling failed:"))))
                          (.setName "crux.dataflow.worker-thread")
                          (.start))]
      (->CruxDataflowTxListener conn db crux-node worker-thread server-process))))
