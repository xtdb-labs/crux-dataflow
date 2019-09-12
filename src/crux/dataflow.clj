(ns crux.dataflow
  (:require
   [clojure.spec.alpha :as s]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as w]
   [clj-3df.core :as df]
   [clj-3df.encode :as dfe]
   [manifold.stream]
   [crux.api :as api]
   [crux.codec :as c]
   [crux.io :as cio]
   [crux.query :as q])
  (:import java.io.Closeable
           java.io.File
           java.util.Date
           [java.util.concurrent BlockingQueue LinkedBlockingQueue]))

(defn- validate-value-type! [value-type v]
  (assert
   (case value-type
     :String (string? v)
     :Number (number? v)
     :Bool (boolean? v)
     :Eid (c/valid-id? v)
     :Aid (keyword? v)
     :Instant (instance? Date v)
     :Uuid (uuid? v)
     :Real (float? v))
   (str value-type " " v)))

(defn- validate-schema! [schema {:keys [crux.db/id] :as doc}]
  (assert (c/valid-id? id))
  (assert (map? doc))
  (doseq [[k v] (dissoc doc :crux.db/id)
          :let [{:keys [db/valueType input_semantics]} (get schema k)]]
    (assert (contains? schema k))
    (case input_semantics
      "CardinalityMany"
      (do (do (assert (coll? v))
              (doseq [i v]
                (validate-value-type! valueType i))))
      "CardinalityOne"
      (validate-value-type! valueType v))))

(defn- matches-schema? [schema doc]
  (try
    (validate-schema! schema doc)
    true
    (catch AssertionError e
      (log/debug e "Does not match schema:")
      false)))

;; TODO: This needs to be consistent and persisted for everything sent
;; to the same 3DF server or cluster.
(defn- get-id->long [id->long id]
  (get (swap! id->long
              (fn [id->long]
                (assoc id->long id (get id->long id (inc (count id->long))))))
       id))

(defn- maybe-replace-id [id->long schema a v]
  (if (and (or (= :crux.db/id a)
               (= :Eid (get-in schema [a :db/valueType])))
           (c/valid-id? v))
    (if (coll? v)
      (->> (for [v v]
             (get-id->long id->long v))
           (into (empty v)))
      (get-id->long id->long v))
    v))

(defn- index-to-3df
  [crux-node conn db id->long schema {:keys [crux.api/tx-ops crux.tx/tx-time crux.tx/tx-id]}]
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
                                      _ (log/debug "NEW-DOC:" (pr-str new-doc))
                                      eid (:crux.db/id new-doc)
                                      eid-long (get-id->long id->long eid)
                                      old-doc (some->> (api/history-descending crux-db snapshot eid)
                                                       ;; NOTE: This comment seems like a potential bug?
                                                       ;; history-descending inconsistently includes the current document
                                                       ;; sometimes (on first transaction attleast
                                                       (filter
                                                        (fn [entry] (not= (:crux.tx/tx-id entry) tx-id)))
                                                       first :crux.db/doc)
                                      _ (log/debug "OLD-DOC:" (pr-str old-doc))]
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
                                        (log/debug "KEY:" k (pr-str old-set) (pr-str new-set))
                                        (concat
                                         (for [new new-set
                                               :when (not (nil? new))
                                               :when (not (contains? old-set new))]
                                           [:db/add eid-long k (maybe-replace-id id->long schema k new)])
                                         (for [old old-set
                                               :when (not (nil? old))
                                               :when (not (contains? new-set old))]
                                           [:db/retract eid-long k (maybe-replace-id id->long schema k old)]))))))))))
             []
             tx-ops)]
        (log/debug "3DF Tx:" (pr-str new-transaction))
        @(df/exec! conn (df/transact db new-transaction))))))

(defrecord CruxDataflowTxListener [conn db schema id->long ^Thread worker-thread ^Process server-process]
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

(defn- dataflow-consumer [crux-node conn db id->long from-tx-id {:crux.dataflow/keys [schema
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
                               (index-to-3df crux-node conn db id->long schema tx-log-entry)
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
        db (df/create-db schema)
        id->long (atom {})]
    (df/exec! conn (df/create-db-inputs db))
    (let [worker-thread (doto (Thread.
                               #(try
                                  (dataflow-consumer crux-node conn db id->long -1 options)
                                  (catch InterruptedException ignore)
                                  (catch Throwable t
                                    (log/fatal t "Polling failed:"))))
                          (.setName "crux.dataflow.worker-thread")
                          (.start))]
      (->CruxDataflowTxListener conn db schema id->long worker-thread server-process))))

(defn- transform-ids [id->long schema clauses]
  (w/postwalk
   (fn [x]
     (if (and (vector? x) (= 3 (count x)))
       (let [[e a v] x]
         [e a (maybe-replace-id id->long schema a v)])
       x))
   clauses))

(defn- transform-result-ids [long->id results]
  (w/postwalk
   (fn [x]
     (if (and (map? x) (= [:Eid] (keys x)))
       (update x :Eid long->id)
       x))
   results))

;; TODO: Listening and execution is split in the lower level API,
;; might resurface that. Here we reuse query-name for both query and
;; listener key.
(defn subscribe-query!
  ^java.util.concurrent.BlockingQueue
  [{:keys [id->long conn db schema] :as dataflow-tx-listener} query-name query]
  (let [query (-> (q/normalize-query query)
                  (update :where #(transform-ids id->long schema %))
                  (update :rules #(transform-ids id->long schema %)))
        queue (LinkedBlockingQueue.)]
    (df/listen-query!
     conn
     query-name
     query-name
     (fn [results]
       ;; TODO: This can map should not be inverted all the time, need
       ;; a reverse mapping.
       (let [long->id (set/map-invert @id->long)
             tuples (->> (for [[tx tx-results] (->> (group-by second (transform-result-ids long->id results))
                                                    (sort-by (comp :TxId key)))
                               :let [tuples (for [[t tx add-delete] tx-results
                                                  :when (= 1 add-delete)]
                                              (mapv dfe/decode-value t))]
                               :when (seq tuples)]
                           [(:TxId tx) (vec tuples)])
                         (into (sorted-map)))]
         (log/debug query-name "updated:" (pr-str results) "tuples:" (pr-str tuples))
         (.put queue tuples))))
    (df/exec! conn
              (df/query
               db
               query-name
               (select-keys query [:find :where])
               (get query :rules [])))
    queue))

(defn unsubscribe-query! [{:keys [conn] :as dataflow-tx-listener} query-name]
  (df/unlisten-query! conn query-name query-name))
