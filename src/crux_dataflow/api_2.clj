(ns crux-dataflow.api-2
  (:require
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [clj-3df.core :as df]
    [clj-3df.encode :as dfe]
    [manifold.stream]
    [crux.api :as api]
    [crux-dataflow.query-altering :as q-alt]
    [crux-dataflow.server-connect :as srv-conn]
    [crux-dataflow.schema :as schema]
    [crux-dataflow.crux-helpers :as f]
    [crux-dataflow.misc-helpers :as fm]
    [crux-dataflow.df-upload :as ingest]
    [clojure.pprint :as pp])
  (:import java.io.Closeable
           [java.util.concurrent LinkedBlockingQueue]
           (clojure.lang IPersistentCollection)))


(def ^:private query->name (atom {}))

(defn- map-query-to-id! [q]
  (if-let [qname (@query->name q)]
    qname
    (let [qname (fm/uuid)]
      (swap! query->name assoc q qname)
      qname)))


(defrecord CruxDataflowTxListener
  [conn df-db crux-node schema ^Thread worker-thread ^Process server-process]
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


(defn- dataflow-consumer
  [crux-node conn df-db from-tx-id
   {:crux.dataflow/keys [schema
                         poll-interval
                         batch-size]
               :or {poll-interval 100
                    batch-size 1000}
               :as options}]
  (loop [tx-id from-tx-id]
    (let [last-tx-id (with-open [tx-log-context (api/new-tx-log-context crux-node)]
                       (->> (api/tx-log crux-node tx-log-context (inc tx-id) true)
                            (take batch-size)
                            (reduce
                             (fn [_ {:keys [crux.tx/tx-id] :as tx-log-entry}]
                               (ingest/upload-single-crux-tx-to-3df crux-node conn df-db schema tx-log-entry)
                               tx-id)
                             tx-id)))]
      (when (= last-tx-id tx-id)
        (Thread/sleep poll-interval))
      (recur (long last-tx-id)))))

(defn start-dataflow-tx-listener
  ^java.io.Closeable [crux-node
                      {:crux.dataflow/keys [schema
                                            url
                                            debug-connection?
                                            embed-server?]
                       :or {url srv-conn/default-dataflow-server-url
                            debug-connection? false
                            embed-server? false}
                       :as options}]
  (s/assert :crux.dataflow/tx-listener-options options)
  (let [server-process (when embed-server?
                         (srv-conn/start-dataflow-server))
        conn ((if debug-connection?
                df/create-debug-conn!
                df/create-conn!) url)
        df-db (df/create-db schema)
        from-tx-id (inc (f/latest-tx-id crux-node))]
    (df/exec! conn (df/create-db-inputs df-db))
    (let [worker-thread (doto (Thread.
                               #(try
                                  (dataflow-consumer crux-node conn df-db from-tx-id options)
                                  (catch InterruptedException ignore)
                                  (catch Throwable t
                                    (log/fatal t "Polling failed:"))))
                          (.setName "crux-dataflow.worker-thread")
                          (.start))]
      (->CruxDataflowTxListener conn df-db crux-node schema worker-thread server-process))))


(defn submit-query! [{:keys [conn db schema] :as dataflow-tx-listener} query-name query-prepared]
  (df/exec! conn
            (df/query
              db
              query-name
              (select-keys query-prepared [:find :where])
              (get query-prepared :rules []))))

(defn- mk-listener--raw
  [query-name
   {:crux.dataflow/keys [query]}
   queue]
  (fn on-3df-message [results]
    (log/debug "RESULTS" query-name results)
    (let [tuples (->> (for [[tx tx-results] (->> (group-by second (schema/decode-result-ids results))
                                                 (sort-by (comp :TxId key)))
                            :let [tuples (for [[t tx add-delete] tx-results
                                               :when (= 1 add-delete)]
                                           (mapv dfe/decode-value t))]
                            :when (seq tuples)]
                        [(:TxId tx) (vec tuples)])
                      (into (sorted-map)))]
      (log/debug query-name "updated:" (pr-str results) "tuples:" (pr-str tuples))
      (.put queue tuples))))

(comment
  ["two"
   (([{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}]
     {:TxId 19}
     1)
    ([{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}]
     {:TxId 19}
     3)
    ([{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1)
    ([{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3))])

(def raw-res
  [[[{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}]
    {:TxId 19}
    1]
   [[{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}]
    {:TxId 19}
    3]
   [[{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1]
   [[{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]])

(->> (group-by (comp :TxId second) raw-res)
     (sort-by key))
; (1) yields
(def df-triplet-batches
  [[19
    [[[{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}] {:TxId 19} 1]
     [[{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}] {:TxId 19} 3] ; addition triplet
     [[{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1] ; retraction triplet
     [[{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]]]])

(defn- df-triplet-type [[_ _ ^Long cardinality-delta :as df-diff-tuple]]
  (if (> cardinality-delta 0)
    :crux.df/added
    :crux.df/deleted))

(defn decode-tuple-values [tuple-values]
  (mapv dfe/decode-value tuple-values))


(defn shape-batch--vector [schema query tx-id triplets-batch]
  (->> triplets-batch
       (group-by df-triplet-type)
       (fm/map-values #(mapv (comp decode-tuple-values first) %))))

(assert
  (= {:crux.df/added [["pafoewijfewoijfwehhhhh" "hofiewjoiwef"]
                      ["pafoewijfewoijfwehhhhh" "hofijoiwef"]],
      :crux.df/deleted [["pak" "hofiewjoiwef"] ["pak" "hofijoiwef"]]}
     (shape-batch--vector
       schema/test-schema
       '{:find [?name ?email]
         :where
         [[?user :user/name ?name]
          [?user :user/email ?email]]}
       19
       [[[{:String "pafoewijfewoijfwehhhhh"} {:String "hofiewjoiwef"}] {:TxId 19} 1]
        [[{:String "pafoewijfewoijfwehhhhh"} {:String "hofijoiwef"}] {:TxId 19} 3]
        [[{:String "pak"} {:String "hofiewjoiwef"}] {:TxId 19} -1]
        [[{:String "pak"} {:String "hofijoiwef"}] {:TxId 19} -3]])))

(defn- mk-listener--shaping
  [schema
   query-name
   {:crux.dataflow/keys [query]}
   queue]
  (fn on-3df-message [results]
    (log/debug "RESULTS" query-name (fm/pp-str results))
    (let [ordered-result-triplets ; (1)
          (->> (group-by (comp :TxId second) results)
               (sort-by key))
          shaped-results-by-tx
          (for [[tx-id triplets-batch] ordered-result-triplets]
            (shape-batch--vector schema query, tx-id triplets-batch))]
      (doseq [diff-op shaped-results-by-tx]
        (.put queue diff-op)))))

(defn query-entities [crux-node query]
  (let [fr-query (q-alt/entities-grabbing-alteration query)]
    (mapv first (api/q (api/db crux-node) fr-query))))

(defn transact-data-for-query!
  [{:keys [crux-node] :as df-listener} query]
  (let [results (query-entities crux-node query)]
    (ingest/upload-crux-query-results df-listener results)))

(defn subscribe-query!
  ^java.util.concurrent.BlockingQueue
  [{:keys [conn schema] :as df-listener}
   {:crux.dataflow/keys [sub-id query query-name] :as opts}]
  (let [query--prepared (schema/prepare-query schema query)
        query-name (or query-name (map-query-to-id! query--prepared))
        queue (LinkedBlockingQueue.)]
    (transact-data-for-query! df-listener query)
    (submit-query! df-listener query-name query--prepared)
    (df/listen-query! conn query-name sub-id (mk-listener--shaping schema query-name opts queue))
    queue))

(defn unsubscribe-query! [{:keys [conn] :as dataflow-tx-listener} query-name]
  (df/unlisten-query! conn query-name query-name))
