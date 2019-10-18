(ns crux-dataflow.api-2
  (:require
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [clj-3df.core :as df]
    [clj-3df.encode :as dfe]
    [manifold.stream]
    [crux.api :as api]
    [crux-dataflow.server-connect :as srv-conn]
    [crux-dataflow.schema :as schema]
    [crux-dataflow.crux-helpers :as f]
    [crux-dataflow.misc-helpers :as fm]
    [crux-dataflow.3df-ingest :as ingest]
    [crux.query :as q]
    [clojure.pprint :as pp])
  (:import java.io.Closeable
           [java.util.concurrent LinkedBlockingQueue]))


(defrecord CruxDataflowTxListener
  [conn db schema ^Thread worker-thread ^Process server-process]
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


(defn- dataflow-consumer [crux-node conn db from-tx-id
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
                               (ingest/index-to-3df crux-node conn db schema tx-log-entry)
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
        db (df/create-db schema)
        from-tx-id (inc (f/latest-tx-id crux-node))]
    (df/exec! conn (df/create-db-inputs db))
    (let [worker-thread (doto (Thread.
                               #(try
                                  (dataflow-consumer crux-node conn db from-tx-id options)
                                  (catch InterruptedException ignore)
                                  (catch Throwable t
                                    (log/fatal t "Polling failed:"))))
                          (.setName "crux.dataflow.worker-thread")
                          (.start))]
      (->CruxDataflowTxListener conn db schema worker-thread server-process))))

(def ^:private query->name (atom {}))

(defn- compute-query-name! [q]
  (if-let [qname (@query->name q)]
    qname
    (let [qname (fm/uuid)]
      (swap! query->name assoc q qname)
      qname)))

(defn subscribe-query!
  ^java.util.concurrent.BlockingQueue
  [{:keys [conn db schema] :as dataflow-tx-listener}
   {:crux.dataflow/keys [sub-id query]}]
  (let [query-n (-> (q/normalize-query query)
                    (update :where #(schema/encode-query-ids schema %))
                    (update :rules #(schema/encode-query-ids schema %)))
        query-name (compute-query-name! query-n)
        queue (LinkedBlockingQueue.)]
    (df/listen-query!
     conn
     query-name
     sub-id
     (fn [results]
       (pp/pprint "hmhm" results)
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
    (df/exec! conn
              (df/query
                db
                query-name
                (select-keys query-n [:find :where])
                (get query-n :rules [])))
    queue))

(defn unsubscribe-query! [{:keys [conn] :as dataflow-tx-listener} query-name]
  (df/unlisten-query! conn query-name query-name))
